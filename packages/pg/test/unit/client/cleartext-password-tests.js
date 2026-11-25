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

const helper = require('./test-helper')
const createClient = require('./test-helper').createClient

test('cleartext password authentication', function () {
  test('responds with password', function () {
    var client = createClient()
    client.password = '!'
    client.connection.stream.packets = []
    client.connection.emit('authenticationCleartextPassword')
    var packets = client.connection.stream.packets
    assert.lengthIs(packets, 1)
    var packet = packets[0]
    assert.equalBuffers(packet, [0x70, 0, 0, 0, 6, 33, 0])
  })

  test('does not crash with null password using pg-pass', function () {
    process.env.PGPASSFILE = `${__dirname}/pgpass.file`
    var client = new helper.Client({
      host: 'foo',
      port: 5432,
      database: 'bar',
      user: 'baz',
      stream: new MemoryStream(),
    })
    client.connect()
    client.connection.emit('authenticationCleartextPassword')
  })
})
