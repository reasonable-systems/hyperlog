const hyperlog = require('../')
const tape = require('tape')
const memdb = require('memdb')

tape('sign', function (t) {
  t.plan(4)

  const log = hyperlog(memdb(), {
    identity: Buffer.from('i-am-a-public-key'),
    sign: function (node, cb) {
      t.same(node.value, Buffer.from('hello'), 'sign is called')
      cb(null, Buffer.from('i-am-a-signature'))
    }
  })

  log.add(null, 'hello', function (err, node) {
    t.error(err, 'no err')
    t.same(node.signature, Buffer.from('i-am-a-signature'), 'has signature')
    t.same(node.identity, Buffer.from('i-am-a-public-key'), 'has public key')
    t.end()
  })
})

tape('sign fails', function (t) {
  t.plan(2)

  const log = hyperlog(memdb(), {
    identity: Buffer.from('i-am-a-public-key'),
    sign: function (node, cb) {
      cb(new Error('lol'))
    }
  })

  log.on('reject', function (node) {
    t.ok(node)
  })

  log.add(null, 'hello', function (err) {
    t.same(err && err.message, 'lol', 'had error')
  })
})

tape('verify', function (t) {
  t.plan(3)

  const log1 = hyperlog(memdb(), {
    identity: Buffer.from('i-am-a-public-key'),
    sign: function (node, cb) {
      cb(null, Buffer.from('i-am-a-signature'))
    }
  })

  const log2 = hyperlog(memdb(), {
    verify: function (node, cb) {
      t.same(node.signature, Buffer.from('i-am-a-signature'), 'verify called with signature')
      t.same(node.identity, Buffer.from('i-am-a-public-key'), 'verify called with public key')
      cb(null, true)
    }
  })

  log1.add(null, 'hello', function (err, node) {
    t.error(err, 'no err')
    const stream = log2.replicate()
    stream.pipe(log1.replicate()).pipe(stream)
  })
})

tape('verify fails', function (t) {
  t.plan(2)

  const log1 = hyperlog(memdb(), {
    identity: Buffer.from('i-am-a-public-key'),
    sign: function (node, cb) {
      cb(null, Buffer.from('i-am-a-signature'))
    }
  })

  const log2 = hyperlog(memdb(), {
    verify: function (node, cb) {
      cb(null, false)
    }
  })

  log1.add(null, 'hello', function (err, node) {
    t.error(err, 'no err')

    const stream = log2.replicate()

    stream.on('error', function (err) {
      t.same(err.message, 'Invalid signature', 'stream had error')
      t.end()
    })
    stream.pipe(log1.replicate()).pipe(stream)
  })
})

tape('per-document identity (add)', function (t) {
  t.plan(3)

  const log1 = hyperlog(memdb(), {
    sign: function (node, cb) {
      cb(null, Buffer.from('i-am-a-signature'))
    }
  })

  const log2 = hyperlog(memdb(), {
    verify: function (node, cb) {
      t.same(node.signature, Buffer.from('i-am-a-signature'), 'verify called with signature')
      t.same(node.identity, Buffer.from('i-am-a-public-key'), 'verify called with public key')
      cb(null, true)
    }
  })

  const opts = { identity: Buffer.from('i-am-a-public-key') }
  log1.add(null, 'hello', opts, function (err, node) {
    t.error(err, 'no err')
    const stream = log2.replicate()
    stream.pipe(log1.replicate()).pipe(stream)
  })
})

tape('per-document identity (batch)', function (t) {
  t.plan(5)

  const log1 = hyperlog(memdb(), {
    sign: function (node, cb) {
      cb(null, Buffer.from('i-am-a-signature'))
    }
  })

  const expectedpk = [Buffer.from('hello id'), Buffer.from('whatever id')]
  const log2 = hyperlog(memdb(), {
    verify: function (node, cb) {
      t.same(node.signature, Buffer.from('i-am-a-signature'), 'verify called with signature')
      t.same(node.identity, expectedpk.shift(), 'verify called with public key')
      cb(null, true)
    }
  })

  log1.batch([
    {
      value: 'hello',
      identity: Buffer.from('hello id')
    },
    {
      value: 'whatever',
      identity: Buffer.from('whatever id')
    }
  ], function (err, nodes) {
    t.error(err, 'no err')
    const stream = log2.replicate()
    stream.pipe(log1.replicate()).pipe(stream)
  })
})
