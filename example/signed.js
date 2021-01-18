const hyperlog = require('../')
const memdb = require('memdb')
const sodium = require('sodium').api
const eq = require('buffer-equals')

const keys = sodium.crypto_sign_keypair()
const log = hyperlog(memdb(), {
  identity: keys.publicKey,
  sign: function (node, cb) {
    const bkey = Buffer.from(node.key, 'hex')
    cb(null, sodium.crypto_sign(bkey, keys.secretKey))
  }
})
const clone = hyperlog(memdb(), {
  verify: function (node, cb) {
    if (!node.signature) return cb(null, false)
    if (!eq(node.identity, keys.publicKey)) return cb(null, false)
    const bkey = Buffer.from(node.key, 'hex')
    const m = sodium.crypto_sign_open(node.signature, node.identity)
    cb(null, eq(m, bkey))
  }
})

const sync = function (a, b) {
  a = a.createReplicationStream({ mode: 'push' })
  b = b.createReplicationStream({ mode: 'pull' })

  a.on('push', function () {
    console.log('a pushed')
  })

  a.on('pull', function () {
    console.log('a pulled')
  })

  a.on('end', function () {
    console.log('a ended')
  })

  b.on('push', function () {
    console.log('b pushed')
  })

  b.on('pull', function () {
    console.log('b pulled')
  })

  b.on('end', function () {
    console.log('b ended')
  })

  a.pipe(b).pipe(a)
}

clone.createReadStream({ live: true }).on('data', function (data) {
  console.log('change: (%d) %s', data.change, data.key)
})

log.add(null, 'hello', function (err, node) {
  if (err) throw err
  log.add(node, 'world', function (err, node) {
    if (err) throw err
    sync(log, clone)
    log.add(null, 'meh')
  })
})
