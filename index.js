const after = require('after-all')
const lexint = require('lexicographic-integer')
const collect = require('stream-collector')
const through = require('through2')
const pump = require('pump')
const from = require('from2')
const mutexify = require('mutexify')
const cuid = require('cuid')
const logs = require('level-logs')
const events = require('events')
const util = require('util')
const enumerate = require('level-enumerate')
const replicate = require('./lib/replicate')
const messages = require('./lib/messages')
const hash = require('./lib/hash')
const encoder = require('./lib/encode')
const defined = require('defined')
const parallel = require('run-parallel')
const waterfall = require('run-waterfall')

const ID = '!!id'
const CHANGES = '!changes!'
const NODES = '!nodes!'
const HEADS = '!heads!'

const INVALID_SIGNATURE = new Error('Invalid signature')
const CHECKSUM_MISMATCH = new Error('Checksum mismatch')
const INVALID_LOG = new Error('Invalid log sequence')

INVALID_LOG.notFound = true
INVALID_LOG.status = 404

const noop = function () {}

const Hyperlog = function (db, opts) {
  if (!(this instanceof Hyperlog)) return new Hyperlog(db, opts)
  if (!opts) opts = {}

  events.EventEmitter.call(this)

  this.id = defined(opts.id, null)
  this.enumerate = enumerate(db, { prefix: 'enum' })
  this.db = db
  this.logs = logs(db, { prefix: 'logs', valueEncoding: messages.Entry })
  this.lock = defined(opts.lock, mutexify())
  this.changes = 0
  this.setMaxListeners(0)
  this.valueEncoding = defined(opts.valueEncoding, opts.encoding, 'binary')
  this.identity = defined(opts.identity, null)
  this.verify = defined(opts.verify, null)
  this.sign = defined(opts.sign, null)
  this.hash = defined(opts.hash, hash)
  this.asyncHash = defined(opts.asyncHash, null)

  // Retrieve this hyperlog instance's unique ID.
  const self = this
  const getId = defined(opts.getId, function (cb) {
    db.get(ID, { valueEncoding: 'utf-8' }, function (_, id) {
      if (id) return cb(null, id)
      id = cuid()
      db.put(ID, id, function () {
        cb(null, id)
      })
    })
  })

  // Startup logic to..
  // 1. Determine & record the largest change # in the db.
  // 2. Determine this hyperlog db's local ID.
  //
  // This is behind a lock in order to ensure that no hyperlog operations
  // can be performed -- these two values MUST be known before any
  // hyperlog usage may occur.
  this.lock(function (release) {
    collect(db.createKeyStream({ gt: CHANGES, lt: CHANGES + '~', reverse: true, limit: 1 }), function (_, keys) {
      self.changes = Math.max(self.changes, keys && keys.length ? lexint.unpack(keys[0].split('!').pop(), 'hex') : 0)
      if (self.id) return release()
      getId(function (_, id) {
        self.id = id || cuid()
        release()
      })
    })
  })
}

util.inherits(Hyperlog, events.EventEmitter)

// Call callback 'cb' once the hyperlog is ready for use (knows some
// fundamental properties about itself from the leveldb). If it's already
// ready, cb is called immediately.
Hyperlog.prototype.ready = function (cb) {
  if (this.id) return cb()
  this.lock(function (release) {
    release()
    cb()
  })
}

// Returns a readable stream of all hyperlog heads. That is, all nodes that no
// other nodes link to.
Hyperlog.prototype.heads = function (opts, cb) {
  const self = this
  if (!opts) opts = {}
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }

  const rs = this.db.createValueStream({
    gt: HEADS,
    lt: HEADS + '~',
    valueEncoding: 'utf-8'
  })

  const format = through.obj(function (key, enc, cb) {
    self.get(key, opts, cb)
  })

  return collect(pump(rs, format), cb)
}

// Retrieve a single, specific node, given its key.
Hyperlog.prototype.get = function (key, opts, cb) {
  if (!opts) opts = {}
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  const self = this
  this.db.get(NODES + key, { valueEncoding: 'binary' }, function (err, buf) {
    if (err) return cb(err)
    const node = messages.Node.decode(buf)
    node.value = encoder.decode(node.value, opts.valueEncoding || self.valueEncoding)
    cb(null, node)
  })
}

// Utility function to be used in a nodes.reduce() to determine the largest
// change # present.
const maxChange = function (max, cur) {
  return Math.max(max, cur.change)
}

// Consumes either a string or a hyperlog node and returns its key.
const toKey = function (link) {
  return typeof link !== 'string' ? link.key : link
}

// Adds a new hyperlog node to an existing array of leveldb batch insertions.
// This includes performing crypto signing and verification.
// Performs deduplication; returns the existing node if alreay present in the hyperlog.
const addBatchAndDedupe = function (dag, node, logLinks, batch, opts, cb) {
  if (opts.hash && node.key !== opts.hash) return cb(CHECKSUM_MISMATCH)
  if (opts.seq && node.seq !== opts.seq) return cb(INVALID_LOG)

  const log = {
    change: node.change,
    node: node.key,
    links: logLinks
  }

  const onclone = function (clone) {
    if (!opts.log) return cb(null, clone, [])
    batch.push({ type: 'put', key: dag.logs.key(node.log, node.seq), value: messages.Entry.encode(log) })
    cb(null, clone)
  }

  const done = function () {
    dag.get(node.key, { valueEncoding: 'binary' }, function (_, clone) {
      // This node already exists somewhere in the hyperlog; add it to the
      // log's append-only log, but don't insert it again.
      if (clone) return onclone(clone)

      const links = node.links
      for (let i = 0; i < links.length; i++) batch.push({ type: 'del', key: HEADS + links[i] })
      batch.push({ type: 'put', key: CHANGES + lexint.pack(node.change, 'hex'), value: node.key })
      batch.push({ type: 'put', key: NODES + node.key, value: messages.Node.encode(node) })
      batch.push({ type: 'put', key: HEADS + node.key, value: node.key })
      batch.push({ type: 'put', key: dag.logs.key(node.log, node.seq), value: messages.Entry.encode(log) })

      cb(null, node)
    })
  }

  // Local node; sign it.
  if (node.log === dag.id) {
    if (!dag.sign || node.signature) return done()
    dag.sign(node, function (err, sig) {
      if (err) return cb(err)
      if (!node.identity) node.identity = dag.identity
      node.signature = sig
      done()
    })
  // Remote node; verify it.
  } else {
    if (!dag.verify) return done()
    dag.verify(node, function (err, valid) {
      if (err) return cb(err)
      if (!valid) return cb(INVALID_SIGNATURE)
      done()
    })
  }
}

const getLinks = function (dag, id, links, cb) {
  const logLinks = []
  const nextLink = function () {
    const cb = next()
    return function (err, link) {
      if (err) return cb(err)
      if (link.log !== id && logLinks.indexOf(link.log) === -1) logLinks.push(link.log)
      cb(null)
    }
  }
  const next = after(function (err) {
    if (err) cb(err)
    else cb(null, logLinks)
  })

  for (let i = 0; i < links.length; i++) {
    dag.get(links[i], nextLink())
  }
}

// Produce a readable stream of all nodes added from this point onward, in
// topographic order.
const createLiveStream = function (dag, opts) {
  let since = opts.since || 0
  let limit = opts.limit || -1
  let wait = null

  const read = function (size, cb) {
    if (dag.changes <= since) {
      wait = cb
      return
    }

    if (!limit) return cb(null, null)

    dag.db.get(CHANGES + lexint.pack(since + 1, 'hex'), function (err, hash) {
      if (err) return cb(err)
      dag.get(hash, opts, function (err, node) {
        if (err) return cb(err)
        since = node.change
        if (limit !== -1) limit--
        cb(null, node)
      })
    })
  }

  const kick = function () {
    if (!wait) return
    const cb = wait
    wait = null
    read(0, cb)
  }

  dag.on('add', kick)
  dag.ready(kick)

  const rs = from.obj(read)

  rs.once('close', function () {
    dag.removeListener('add', kick)
  })

  return rs
}

// Produce a readable stream of nodes in the hyperlog, in topographic order.
Hyperlog.prototype.createReadStream = function (opts) {
  if (!opts) opts = {}
  if (opts.tail) {
    opts.since = this.changes
  }
  if (opts.live) return createLiveStream(this, opts)

  const self = this
  const since = opts.since || 0
  const until = opts.until || 0

  const keys = this.db.createValueStream({
    gt: CHANGES + lexint.pack(since, 'hex'),
    lt: CHANGES + (until ? lexint.pack(until, 'hex') : '~'),
    valueEncoding: 'utf-8',
    reverse: opts.reverse,
    limit: opts.limit
  })

  const get = function (key, enc, cb) {
    self.get(key, opts, cb)
  }

  return pump(keys, through.obj(get))
}

Hyperlog.prototype.replicate =
Hyperlog.prototype.createReplicationStream = function (opts) {
  return replicate(this, opts)
}

Hyperlog.prototype.add = function (links, value, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!cb) cb = noop
  this.batch([{ links: links, value: value }], opts, function (err, nodes) {
    if (err) cb(err)
    else cb(null, nodes[0])
  })
}

Hyperlog.prototype.batch = function (docs, opts, cb) {
  // 0. preamble
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!cb) cb = noop
  if (!opts) opts = {}

  // Bail asynchronously; nothing to add.
  if (docs.length === 0) return process.nextTick(function () { cb(null, []) })

  const self = this
  const id = opts.log || self.id
  opts.log = id

  const logLinks = {}
  let lockRelease = null
  let latestSeq

  // Bubble up errors on non-batch (1 element) calls.
  let bubbleUpErrors = false
  if (docs.length === 1) {
    bubbleUpErrors = true
  }

  // 1. construct initial hyperlog "node" for each of "docs"
  const nodes = docs.map(function (doc) {
    return constructInitialNode(doc, opts)
  })

  // 2. emit all preadd events
  nodes.forEach(function (node) {
    self.emit('preadd', node)
  })

  waterfall([
    // 3. lock the hyperlog (if needed)
    // 4. wait until the hyperlog is 'ready'
    // 5. retrieve the seq# of this hyperlog's head
    lockAndGetSeqNumber,

    // 3. hash (async/sync) all nodes
    // 4. retrieve + set 'getLinks' for each node
    function (seq, release, done) {
      lockRelease = release
      latestSeq = seq

      hashNodesAndFindLinks(nodes, done)
    },

    // 8. dedupe the node against the params AND the hyperlog (in sequence)
    function (nodes, done) {
      dedupeNodes(nodes, latestSeq, done)
    },

    // 9. create each node's leveldb batch operation object
    function (nodes, done) {
      computeBatchNodeOperations(nodes, done)
    },

    // 10. perform the leveldb batch op
    function (nodes, batchOps, done) {
      self.db.batch(batchOps, function (err) {
        if (err) {
          nodes.forEach(rejectNode)
          return done(err)
        }
        done(null, nodes)
      })
    },

    // 11. update the hyperlog's change#
    // 12. emit all add/reject events
    function (nodes, done) {
      self.changes = nodes.reduce(maxChange, self.changes)
      done(null, nodes)
    }
  ], function (err, nodes) {
    // release lock, if necessary
    if (lockRelease) return lockRelease(onUnlocked, err)
    onUnlocked(err)

    function onUnlocked (err) {
      // Error; all nodes were rejected.
      if (err) return cb(err)

      // Emit add events.
      nodes.forEach(function (node) {
        self.emit('add', node)
      })

      cb(null, nodes)
    }
  })

  function rejectNode (node) {
    self.emit('reject', node)
  }

  // Hashes and finds links for the given nodes. If some nodes fail to hash to
  // have their links found, they are rejected and not returned in the results.
  function hashNodesAndFindLinks (nodes, done) {
    const goodNodes = []

    parallel(
      nodes.map(function (node) {
        return function (done) {
          hashNode(node, function (err, key) {
            if (err) {
              rejectNode(node)
              return done(bubbleUpErrors ? err : null)
            }
            node.key = key

            getLinks(self, id, node.links, function (err, links) {
              if (err) {
                rejectNode(node)
                return done(bubbleUpErrors ? err : null)
              }
              logLinks[node.key] = links

              if (!node.log) node.log = self.id

              goodNodes.push(node)
              done()
            })
          })
        }
      }),
      function (err) {
        done(err, goodNodes)
      }
    )
  }

  function lockAndGetSeqNumber (done) {
    if (opts.release) onlocked(opts.release)
    else self.lock(onlocked)

    function onlocked (release) {
      self.ready(function () {
        self.logs.head(id, function (err, seq) {
          if (err) return release(cb, err)
          done(null, seq, release)
        })
      })
    }
  }

  function dedupeNodes (nodes, seq, done) {
    const goodNodes = []

    const added = nodes.length > 1 ? {} : null
    let seqIdx = 1
    let changeIdx = 1

    waterfall(
      nodes.map(function (node) {
        return function (done) {
          dedupeNode(node, done)
        }
      }),
      function (err) {
        done(err, goodNodes)
      }
    )

    function dedupeNode (node, done) {
      // Check if the to-be-added node already exists in the hyperlog.
      self.get(node.key, function (_, clone) {
        // It already exists
        if (clone) {
          node.seq = seq + (seqIdx++)
          node.change = clone.change
        // It already exists; it was added in this batch op earlier on.
        } else if (added && added[node.key]) {
          node.seq = added[node.key].seq
          node.change = added[node.key].change
          rejectNode(node)
          return done()
        } else {
          // new node across all logs
          node.seq = seq + (seqIdx++)
          node.change = self.changes + (changeIdx++)
        }

        if (added) added[node.key] = node

        goodNodes.push(node)

        done()
      })
    }
  }

  function computeBatchNodeOperations (nodes, done) {
    let batch = []
    const goodNodes = []

    waterfall(
      nodes.map(function (node) {
        return function (done) {
          computeNodeBatchOp(node, function (err, ops) {
            if (err) {
              rejectNode(node)
              return done(bubbleUpErrors ? err : null)
            }
            batch = batch.concat(ops)
            goodNodes.push(node)
            done()
          })
        }
      }),
      function (err) {
        if (err) return done(err)
        done(null, nodes, batch)
      }
    )

    // Create a new leveldb batch operation for this node.
    function computeNodeBatchOp (node, done) {
      const batch = []
      const links = logLinks[node.key]
      addBatchAndDedupe(self, node, links, batch, opts, function (err, newNode) {
        if (err) return done(err)
        newNode.value = encoder.decode(newNode.value, opts.valueEncoding || self.valueEncoding)
        done(null, batch)
      })
    }
  }

  function constructInitialNode (doc, opts) {
    let links = doc.links || []
    if (!Array.isArray(links)) links = [links]
    links = links.map(toKey)

    const encodedValue = encoder.encode(doc.value, opts.valueEncoding || self.valueEncoding)
    return {
      log: opts.log || self.id,
      key: null,
      identity: doc.identity || opts.identity || null,
      signature: opts.signature || null,
      value: encodedValue,
      links: links
    }
  }

  function hashNode (node, done) {
    if (self.asyncHash) {
      self.asyncHash(node.links, node.value, done)
    } else {
      const key = self.hash(node.links, node.value)
      done(null, key)
    }
  }
}

Hyperlog.prototype.append = function (value, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!cb) cb = noop
  if (!opts) opts = {}
  const self = this

  this.lock(function (release) {
    self.heads(function (err, heads) {
      if (err) return release(cb, err)
      opts.release = release
      self.add(heads, value, opts, cb)
    })
  })
}

module.exports = Hyperlog
