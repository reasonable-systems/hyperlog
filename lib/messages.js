const protobuf = require('protocol-buffers')
const fs = require('fs')
const path = require('path')

module.exports = protobuf(fs.readFileSync(path.join(__dirname, '..', 'schema.proto'), 'utf-8'))
