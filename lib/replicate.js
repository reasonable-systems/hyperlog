const through = require('through2')
const pump = require('pump')
const bitfield = require('bitfield')
const protocol = require('./protocol')
const sortedQueue = require('./sorted-queue')
const encoder = require('./encode.js')

const noop = function () {}
const noarr = []

const MAX_BITFIELD = 10 * 1024 * 1024 // arbitrary high number

module.exports = function (dag, opts) {
  if (!opts) opts = {}

  const stream = protocol(opts)
  const mode = opts.mode || 'sync'

  // Bitfield to ensure that the nodes of each log in the hyperlog is only sent
  // once.
  const pushing = bitfield(1024, { grow: MAX_BITFIELD })

  // The largest change # known to this log when replication begins.
  let changes = 0

  let missing = 0

  let done = false
  let remoteSentWants = false
  let remoteSentHeads = false
  let localSentWants = false
  let localSentHeads = false

  let live = opts.live

  // Local nodes yet to be sent.
  const outgoing = sortedQueue()
  // Remote nodes yet to be added to this hyperlog.
  const incoming = sortedQueue()

  // Asynchronous loop to continue sending nodes from a log in sequence from
  // low seq # to its highest seq #.
  outgoing.pull(function loop (entry) {
    dag.get(entry.node, { valueEncoding: 'binary' }, function (err, node) {
      if (err) return stream.destroy(err)

      if (entry.log && (node.log !== entry.log || node.seq !== entry.seq)) { // deduplicated
        node.log = entry.log
        node.seq = entry.seq
      }

      stream.emit('push')
      stream.node(node, function (err) {
        if (err) return stream.destroy(err)
        sendNode(node.log, node.seq + 1, function (err) {
          if (err) return stream.destroy(err)
          outgoing.pull(loop)
        })
      })
    })
  })

  const pipe = function (a, b, cb) {
    const destroy = function () {
      a.destroy()
    }

    stream.on('close', destroy)
    stream.on('finish', destroy)

    a.on('end', function () {
      stream.removeListener('close', destroy)
      stream.removeListener('finish', destroy)
    })

    return pump(a, b, cb)
  }

  // For live replication. Reads live from the local hyperlog and continues to
  // send new nodes to the other end.
  const sendChanges = function () {
    const write = function (node, enc, cb) {
      node.value = encoder.encode(node.value, dag.valueEncoding)
      stream.node(node, cb)
    }

    stream.emit('live')
    pipe(dag.createReadStream({ since: changes, live: true }), through.obj(write))
  }

  // Check if replication is finished.
  const update = function (cb) {
    if (done || !localSentWants || !localSentHeads || !remoteSentWants || !remoteSentHeads) return cb()
    done = true
    if (!live) return stream.finalize(cb)
    sendChanges()
    cb()
  }

  // Inform the other side that we've requested all of the nodes we want.
  const sentWants = function (cb) {
    localSentWants = true
    stream.sentWants()
    update(cb)
  }

  // Inform the other side that we've sent all of the heads we have.
  const sentHeads = function (cb) {
    localSentHeads = true
    stream.sentHeads()
    update(cb)
  }

  // Send a specific entry in a specific log to the other side.
  // If the node links to other nodes, inform the other side we have those,
  // too.
  const sendNode = function (log, seq, cb) {
    dag.logs.get(log, seq, function (err, entry) {
      if (err && err.notFound) return cb()
      if (err) return cb(err)
      if (entry.change > changes) return cb() // ensure snapshot

      entry.log = log
      entry.seq = seq

      let i = 0
      const loop = function () {
        if (i < entry.links.length) return sendHave(entry.links[i++], loop)
        entry.links = noarr // premature opt: less mem yo
        outgoing.push(entry, cb)
      }

      loop()
    })
  }

  // Add a received remote node to our local hyperlog.
  // It is normal for the insertion to sometimes fail: we may have received a
  // node that depends on another node from a log we haven't yet received. If
  // so, enqueue it into 'incoming' and continue trying to re-insert it until
  // its dependencies are also present.
  const receiveNode = function (node, cb) {
    const opts = {
      hash: node.key,
      log: node.log,
      seq: node.seq,
      identity: node.identity,
      signature: node.signature,
      valueEncoding: 'binary'
    }
    dag.add(node.links, node.value, opts, function (err) {
      if (!err) return afterAdd(cb)
      if (!err.notFound) return cb(err)
      incoming.push(node, cb)
    })
  }

  const afterAdd = function (cb) {
    stream.emit('pull')
    if (!localSentWants && !--missing) return sentWants(cb)
    if (!incoming.length) return cb()
    incoming.pull(function (node) {
      receiveNode(node, cb)
    })
  }

  const sendHave = function (log, cb) {
    dag.enumerate(log, function (err, idx) {
      if (err) return cb(err)

      // Don't send the same log twice.
      if (pushing.get(idx)) return cb()
      pushing.set(idx, true)

      dag.logs.head(log, function (err, seq) {
        if (err) return cb(err)
        dag.logs.get(log, seq, function loop (err, entry) { // ensure snapshot
          if (err && err.notFound) return cb()
          if (err) return cb(err)
          if (entry.change > changes) return dag.logs.get(log, seq - 1, loop)
          stream.have({ log: log, seq: seq }, cb)
        })
      })
    })
  }

  stream.once('sentHeads', function (cb) {
    if (!localSentWants && !missing) sentWants(noop)
    remoteSentHeads = true
    update(cb)
  })

  stream.once('sentWants', function (cb) {
    remoteSentWants = true
    update(cb)
  })

  stream.on('want', function (head, cb) {
    sendNode(head.log, head.seq + 1, cb)
  })

  stream.on('have', function (head, cb) {
    dag.logs.head(head.log, function (err, seq) {
      if (err) return cb(err)
      if (seq >= head.seq) return cb()
      missing += (head.seq - seq)
      stream.want({ log: head.log, seq: seq }, cb)
    })
  })

  stream.on('node', receiveNode)

  // start the handshake

  stream.on('handshake', function (handshake, cb) {
    const remoteMode = handshake.mode

    if (remoteMode !== 'pull' && remoteMode !== 'push' && remoteMode !== 'sync') return cb(new Error('Remote uses invalid mode: ' + remoteMode))
    if (remoteMode === 'pull' && mode === 'pull') return cb(new Error('Remote and local are both pulling'))
    if (remoteMode === 'push' && mode === 'push') return cb(new Error('Remote and local are both pushing'))

    remoteSentWants = remoteMode === 'push'
    remoteSentHeads = remoteMode === 'pull'
    localSentWants = mode === 'push' || remoteMode === 'pull'
    localSentHeads = mode === 'pull' || remoteMode === 'push'

    if (handshake.metadata) stream.emit('metadata', handshake.metadata)
    if (!live) live = handshake.live
    if (localSentHeads) return update(cb)

    const write = function (node, enc, cb) {
      sendHave(node.log, cb)
    }

    dag.lock(function (release) { // TODO: don't lock here. figure out how to snapshot the heads to a change instead
      changes = dag.changes
      pipe(dag.heads(), through.obj(write), function (err) {
        release()
        if (err) return cb(err)
        sentHeads(cb)
      })
    })
  })

  stream.handshake({ version: 1, mode: opts.mode, metadata: opts.metadata, live: live })

  return stream
}
