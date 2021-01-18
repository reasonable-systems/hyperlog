const hyperlog = require('../')
const tape = require('tape')
const memdb = require('memdb')
const pump = require('pump')
const through = require('through2')

const sync = function (a, b, cb) {
  const stream = a.replicate()
  pump(stream, b.replicate(), stream, cb)
}

const toJSON = function (log, cb) {
  const map = {}
  log.createReadStream()
    .on('data', function (node) {
      map[node.key] = { value: node.value, links: node.links }
    })
    .on('end', function () {
      cb(null, map)
    })
}

tape('clones', function (t) {
  const hyper = hyperlog(memdb())
  const clone = hyperlog(memdb())

  hyper.add(null, 'a', function () {
    hyper.add(null, 'b', function () {
      hyper.add(null, 'c', function () {
        sync(hyper, clone, function (err) {
          t.error(err)
          toJSON(clone, function (err, map1) {
            t.error(err)
            toJSON(hyper, function (err, map2) {
              t.error(err)
              t.same(map1, map2, 'logs are synced')
              t.end()
            })
          })
        })
      })
    })
  })
})

tape('clones with valueEncoding', function (t) {
  const hyper = hyperlog(memdb(), { valueEncoding: 'json' })
  const clone = hyperlog(memdb(), { valueEncoding: 'json' })

  hyper.add(null, 'a', function () {
    hyper.add(null, 'b', function () {
      hyper.add(null, 'c', function () {
        sync(hyper, clone, function (err) {
          t.error(err)
          toJSON(clone, function (err, map1) {
            t.error(err)
            toJSON(hyper, function (err, map2) {
              t.error(err)
              t.same(map1, map2, 'logs are synced')
              t.end()
            })
          })
        })
      })
    })
  })
})

tape('syncs with initial subset', function (t) {
  const hyper = hyperlog(memdb())
  const clone = hyperlog(memdb())

  clone.add(null, 'a', function () {
    hyper.add(null, 'a', function () {
      hyper.add(null, 'b', function () {
        hyper.add(null, 'c', function () {
          sync(hyper, clone, function (err) {
            t.error(err)
            toJSON(clone, function (err, map1) {
              t.error(err)
              toJSON(hyper, function (err, map2) {
                t.error(err)
                t.same(map1, map2, 'logs are synced')
                t.end()
              })
            })
          })
        })
      })
    })
  })
})

tape('syncs with initial superset', function (t) {
  const hyper = hyperlog(memdb())
  const clone = hyperlog(memdb())

  clone.add(null, 'd', function () {
    hyper.add(null, 'a', function () {
      hyper.add(null, 'b', function () {
        hyper.add(null, 'c', function () {
          sync(hyper, clone, function (err) {
            t.error(err)
            toJSON(clone, function (err, map1) {
              t.error(err)
              toJSON(hyper, function (err, map2) {
                t.error(err)
                t.same(map1, map2, 'logs are synced')
                t.end()
              })
            })
          })
        })
      })
    })
  })
})

tape('process', function (t) {
  const hyper = hyperlog(memdb())
  const clone = hyperlog(memdb())

  const process = function (node, enc, cb) {
    setImmediate(function () {
      cb(null, node)
    })
  }

  hyper.add(null, 'a', function () {
    hyper.add(null, 'b', function () {
      hyper.add(null, 'c', function () {
        const stream = hyper.replicate()
        pump(stream, clone.replicate({ process: through.obj(process) }), stream, function () {
          toJSON(clone, function (err, map1) {
            t.error(err)
            toJSON(hyper, function (err, map2) {
              t.error(err)
              t.same(map1, map2, 'logs are synced')
              t.end()
            })
          })
        })
      })
    })
  })
})

// bugfix: previously replication would not terminate
tape('shared history with duplicates', function (t) {
  const hyper1 = hyperlog(memdb())
  const hyper2 = hyperlog(memdb())

  const doc1 = { links: [], value: 'a' }
  const doc2 = { links: [], value: 'b' }

  hyper1.batch([doc1], function (err) {
    t.error(err)
    sync(hyper1, hyper2, function (err) {
      t.error(err)
      hyper2.batch([doc1, doc2], function (err, nodes) {
        t.error(err)
        t.equals(nodes[0].change, 1)
        t.equals(nodes[1].change, 2)
        sync(hyper1, hyper2, function (err) {
          t.error(err)
          t.end()
        })
      })
    })
  })
})
