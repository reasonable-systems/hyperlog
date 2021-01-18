const hyperlog = require('../')
const tape = require('tape')
const memdb = require('memdb')

tape('add and preadd events', function (t) {
  t.plan(13)
  const hyper = hyperlog(memdb())
  const expected = ['hello', 'world']
  const expectedPre = ['hello', 'world']
  const order = []

  hyper.on('add', function (node) {
    // at this point, the event has already been added
    t.equal(node.value.toString(), expected.shift())
    order.push('add ' + node.value)
  })
  hyper.on('preadd', function (node) {
    t.equal(node.value.toString(), expectedPre.shift())
    order.push('preadd ' + node.value)
    hyper.get(node.key, function (err) {
      t.ok(err.notFound)
    })
  })
  hyper.add(null, 'hello', function (err, node) {
    t.error(err)
    hyper.add(node, 'world', function (err, node2) {
      t.error(err)
      t.ok(node2.key, 'has key')
      t.same(node2.links, [node.key], 'has links')
      t.same(node2.value, Buffer.from('world'))
      t.deepEqual(order, [
        'preadd hello',
        'add hello',
        'preadd world',
        'add world'
      ], 'order')
    })
  })
  t.deepEqual(order, ['preadd hello'])
})
