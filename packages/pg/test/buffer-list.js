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

const BufferList = function () {
  this.buffers = []
}
var p = BufferList.prototype

p.add = function (buffer, front) {
  this.buffers[front ? 'unshift' : 'push'](buffer)
  return this
}

p.addInt16 = function (val, front) {
  return this.add(Buffer.from([val >>> 8, val >>> 0]), front)
}

p.getByteLength = function (initial) {
  return this.buffers.reduce(function (previous, current) {
    return previous + current.length
  }, initial || 0)
}

p.addInt32 = function (val, first) {
  return this.add(
    Buffer.from([(val >>> 24) & 0xff, (val >>> 16) & 0xff, (val >>> 8) & 0xff, (val >>> 0) & 0xff]),
    first
  )
}

p.addCString = function (val, front) {
  var len = Buffer.byteLength(val)
  var buffer = Buffer.alloc(len + 1)
  buffer.write(val)
  buffer[len] = 0
  return this.add(buffer, front)
}

p.addString = function (val, front) {
  var len = Buffer.byteLength(val)
  var buffer = Buffer.alloc(len)
  buffer.write(val)
  return this.add(buffer, front)
}

p.addChar = function (char, first) {
  return this.add(Buffer.from(char, 'utf8'), first)
}

p.join = function (appendLength, char) {
  var length = this.getByteLength()
  if (appendLength) {
    this.addInt32(length + 4, true)
    return this.join(false, char)
  }
  if (char) {
    this.addChar(char, true)
    length++
  }
  var result = Buffer.alloc(length)
  var index = 0
  this.buffers.forEach(function (buffer) {
    buffer.copy(result, index, 0)
    index += buffer.length
  })
  return result
}

BufferList.concat = function () {
  var total = new BufferList()
  for (var i = 0; i < arguments.length; i++) {
    total.add(arguments[i])
  }
  return total.join()
}

module.exports = BufferList
