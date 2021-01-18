const hyperlog = require('../')
const tape = require('tape')
const memdb = require('memdb')
const collect = require('stream-collector')

tape('add node', function (t) {
  const hyper = hyperlog(memdb())

  hyper.add(null, 'hello world', function (err, node) {
    t.error(err)
    t.ok(node.key, 'has key')
    t.same(node.links, [])
    t.same(node.value, Buffer.from('hello world'))
    t.end()
  })
})

tape('add node with links', function (t) {
  const hyper = hyperlog(memdb())

  hyper.add(null, 'hello', function (err, node) {
    t.error(err)
    hyper.add(node, 'world', function (err, node2) {
      t.error(err)
      t.ok(node2.key, 'has key')
      t.same(node2.links, [node.key], 'has links')
      t.same(node2.value, Buffer.from('world'))
      t.end()
    })
  })
})

tape('cannot add node with bad links', function (t) {
  const hyper = hyperlog(memdb())

  hyper.add('i-do-not-exist', 'hello world', function (err) {
    t.ok(err, 'had error')
    t.ok(err.notFound, 'not found error')
    t.end()
  })
})

tape('heads', function (t) {
  const hyper = hyperlog(memdb())

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

tape('deduplicates', function (t) {
  const hyper = hyperlog(memdb())

  hyper.add(null, 'hello world', function (err, node) {
    t.error(err)
    hyper.add(null, 'hello world', function (err, node) {
      t.error(err)
      collect(hyper.createReadStream(), function (err, changes) {
        t.error(err)
        t.same(changes.length, 1, 'only one change')
        t.end()
      })
    })
  })
})

tape('deduplicates -- same batch', function (t) {
  const hyper = hyperlog(memdb())

  const doc = { links: [], value: 'hello world' }

  hyper.batch([doc, doc], function (err, nodes) {
    t.error(err)
    collect(hyper.createReadStream(), function (err, changes) {
      t.error(err)
      t.same(changes.length, 1, 'only one change')
      t.same(hyper.changes, 1, 'only one change')
      t.end()
    })
  })
})

tape('bug repro: bad insert links results in correct preadd/add/reject counts', function (t) {
  const hyper = hyperlog(memdb())

  let pending = 0
  hyper.on('preadd', function (node) { pending++ })
  hyper.on('add', function (node) { pending-- })
  hyper.on('reject', function (node) { pending-- })

  hyper.add(['123'], 'hello', function (err, node) {
    t.ok(err)

    t.equal(pending, 0)
    t.end()
  })
})
