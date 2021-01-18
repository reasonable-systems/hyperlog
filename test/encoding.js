const hyperlog = require('../')
const tape = require('tape')
const memdb = require('memdb')
const collect = require('stream-collector')

tape('add node', function (t) {
  const hyper = hyperlog(memdb(), { valueEncoding: 'json' })

  hyper.add(null, { msg: 'hello world' }, function (err, node) {
    t.error(err)
    t.ok(node.key, 'has key')
    t.same(node.links, [])
    t.same(node.value, { msg: 'hello world' })
    t.end()
  })
})

tape('add node with encoding option', function (t) {
  const hyper = hyperlog(memdb())

  hyper.add(null, { msg: 'hello world' }, { valueEncoding: 'json' },
    function (err, node) {
      t.error(err)
      t.ok(node.key, 'has key')
      t.same(node.links, [])
      t.same(node.value, { msg: 'hello world' })
      t.end()
    })
})

tape('append node', function (t) {
  const hyper = hyperlog(memdb(), { valueEncoding: 'json' })

  hyper.append({ msg: 'hello world' }, function (err, node) {
    t.error(err)
    t.ok(node.key, 'has key')
    t.same(node.links, [])
    t.same(node.value, { msg: 'hello world' })
    t.end()
  })
})

tape('append node with encoding option', function (t) {
  const hyper = hyperlog(memdb())

  hyper.append({ msg: 'hello world' }, { valueEncoding: 'json' }, function (err, node) {
    t.error(err)
    t.ok(node.key, 'has key')
    t.same(node.links, [])
    t.same(node.value, { msg: 'hello world' })
    t.end()
  })
})

tape('add node with links', function (t) {
  const hyper = hyperlog(memdb(), { valueEncoding: 'json' })

  hyper.add(null, { msg: 'hello' }, function (err, node) {
    t.error(err)
    hyper.add(node, { msg: 'world' }, function (err, node2) {
      t.error(err)
      t.ok(node2.key, 'has key')
      t.same(node2.links, [node.key], 'has links')
      t.same(node2.value, { msg: 'world' })
      t.end()
    })
  })
})

tape('cannot add node with bad links', function (t) {
  const hyper = hyperlog(memdb(), { valueEncoding: 'json' })

  hyper.add('i-do-not-exist', { msg: 'hello world' }, function (err) {
    t.ok(err, 'had error')
    t.ok(err.notFound, 'not found error')
    t.end()
  })
})

tape('heads', function (t) {
  const hyper = hyperlog(memdb(), { valueEncoding: 'json' })

  hyper.heads(function (err, heads) {
    t.error(err)
    t.same(heads, [], 'no heads yet')
    hyper.add(null, 'a', function (err, node) {
      t.error(err)
      hyper.heads(function (err, heads) {
        t.error(err)
        t.same(heads, [node], 'has head')
        hyper.add(node, 'b', function (err, node2) {
          t.error(err)
          hyper.heads(function (err, heads) {
            t.error(err)
            t.same(heads, [node2], 'new heads')
            t.end()
          })
        })
      })
    })
  })
})

tape('heads with encoding option', function (t) {
  const hyper = hyperlog(memdb())

  hyper.heads({ valueEncoding: 'json' }, function (err, heads) {
    t.error(err)
    t.same(heads, [], 'no heads yet')
    hyper.add(null, 'a', { valueEncoding: 'json' }, function (err, node) {
      t.error(err)
      hyper.heads({ valueEncoding: 'json' }, function (err, heads) {
        t.error(err)
        t.same(heads, [node], 'has head')
        hyper.add(node, 'b', { valueEncoding: 'json' }, function (err, node2) {
          t.error(err)
          hyper.heads({ valueEncoding: 'json' }, function (err, heads) {
            t.error(err)
            t.same(heads, [node2], 'new heads')
            t.end()
          })
        })
      })
    })
  })
})

tape('get', function (t) {
  const hyper = hyperlog(memdb(), { valueEncoding: 'json' })

  hyper.add(null, { msg: 'hello world' }, function (err, node) {
    t.error(err)
    t.ok(node.key, 'has key')
    t.same(node.links, [])
    t.same(node.value, { msg: 'hello world' })
    hyper.get(node.key, function (err, node2) {
      t.ifError(err)
      t.same(node2.value, { msg: 'hello world' })
      t.end()
    })
  })
})

tape('get with encoding option', function (t) {
  const hyper = hyperlog(memdb())

  hyper.add(null, { msg: 'hello world' }, { valueEncoding: 'json' }, function (err, node) {
    t.error(err)
    t.ok(node.key, 'has key')
    t.same(node.links, [])
    t.same(node.value, { msg: 'hello world' })
    hyper.get(node.key, { valueEncoding: 'json' }, function (err, node2) {
      t.ifError(err)
      t.same(node2.value, { msg: 'hello world' })
      t.end()
    })
  })
})

tape('deduplicates', function (t) {
  const hyper = hyperlog(memdb(), { valueEncoding: 'json' })

  hyper.add(null, { msg: 'hello world' }, function (err, node) {
    t.error(err)
    hyper.add(null, { msg: 'hello world' }, function (err, node) {
      t.error(err)
      collect(hyper.createReadStream(), function (err, changes) {
        t.error(err)
        t.same(changes.length, 1, 'only one change')
        t.end()
      })
    })
  })
})

tape('live replication encoding', function (t) {
  t.plan(2)
  const h0 = hyperlog(memdb(), { valueEncoding: 'json' })
  const h1 = hyperlog(memdb(), { valueEncoding: 'json' })
  h1.createReadStream({ live: true })
    .on('data', function (data) {
      t.deepEqual(data.value, { msg: 'hello world' })
    })

  const r0 = h0.replicate({ live: true })
  const r1 = h1.replicate({ live: true })

  h0.add(null, { msg: 'hello world' }, function (err, node) {
    t.error(err)
    r0.pipe(r1).pipe(r0)
  })
})
