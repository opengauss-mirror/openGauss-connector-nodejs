/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

'use strict'
require('./test-helper')
const BufferList = require('./buffer-list')
// http://developer.postgresql.org/pgdocs/postgres/protocol-message-formats.html

var buffers = {}
buffers.readyForQuery = function () {
  return new BufferList().add(Buffer.from('I')).join(true, 'Z')
}

buffers.authenticationOk = function () {
  return new BufferList().addInt32(0).join(true, 'R')
}

buffers.authenticationCleartextPassword = function () {
  return new BufferList().addInt32(3).join(true, 'R')
}

buffers.authenticationMD5Password = function () {
  return new BufferList()
    .addInt32(5)
    .add(Buffer.from([1, 2, 3, 4]))
    .join(true, 'R')
}

buffers.authenticationSASL = function () {
  return new BufferList().addInt32(10).addCString('SCRAM-SHA-256').addCString('').join(true, 'R')
}

buffers.authenticationSASLContinue = function () {
  return new BufferList().addInt32(11).addString('data').join(true, 'R')
}

buffers.authenticationSASLFinal = function () {
  return new BufferList().addInt32(12).addString('data').join(true, 'R')
}

buffers.parameterStatus = function (name, value) {
  return new BufferList().addCString(name).addCString(value).join(true, 'S')
}

buffers.backendKeyData = function (processID, secretKey) {
  return new BufferList().addInt32(processID).addInt32(secretKey).join(true, 'K')
}

buffers.commandComplete = function (string) {
  return new BufferList().addCString(string).join(true, 'C')
}

buffers.rowDescription = function (fields) {
  fields = fields || []
  var buf = new BufferList()
  buf.addInt16(fields.length)
  fields.forEach(function (field) {
    buf
      .addCString(field.name)
      .addInt32(field.tableID || 0)
      .addInt16(field.attributeNumber || 0)
      .addInt32(field.dataTypeID || 0)
      .addInt16(field.dataTypeSize || 0)
      .addInt32(field.typeModifier || 0)
      .addInt16(field.formatCode || 0)
  })
  return buf.join(true, 'T')
}

buffers.dataRow = function (columns) {
  columns = columns || []
  var buf = new BufferList()
  buf.addInt16(columns.length)
  columns.forEach(function (col) {
    if (col == null) {
      buf.addInt32(-1)
    } else {
      var strBuf = Buffer.from(col, 'utf8')
      buf.addInt32(strBuf.length)
      buf.add(strBuf)
    }
  })
  return buf.join(true, 'D')
}

buffers.error = function (fields) {
  return errorOrNotice(fields).join(true, 'E')
}

buffers.notice = function (fields) {
  return errorOrNotice(fields).join(true, 'N')
}

var errorOrNotice = function (fields) {
  fields = fields || []
  var buf = new BufferList()
  fields.forEach(function (field) {
    buf.addChar(field.type)
    buf.addCString(field.value)
  })
  return buf.add(Buffer.from([0])) // terminator
}

buffers.parseComplete = function () {
  return new BufferList().join(true, '1')
}

buffers.bindComplete = function () {
  return new BufferList().join(true, '2')
}

buffers.notification = function (id, channel, payload) {
  return new BufferList().addInt32(id).addCString(channel).addCString(payload).join(true, 'A')
}

buffers.emptyQuery = function () {
  return new BufferList().join(true, 'I')
}

buffers.portalSuspended = function () {
  return new BufferList().join(true, 's')
}

module.exports = buffers
