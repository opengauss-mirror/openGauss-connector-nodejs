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
var helper = require('./test-helper')
const BufferList = require('../../buffer-list')
var utils = require('../../../lib/utils')

test('md5 authentication', function () {
  var client = helper.createClient()
  client.password = '!'
  var salt = Buffer.from([1, 2, 3, 4])
  client.connection.emit('authenticationMD5Password', { salt: salt })

  test('responds', function () {
    assert.lengthIs(client.connection.stream.packets, 1)
    test('should have correct encrypted data', function () {
      var password = utils.postgresMd5PasswordHash(client.user, client.password, salt)
      // how do we want to test this?
      assert.equalBuffers(client.connection.stream.packets[0], new BufferList().addCString(password).join(true, 'p'))
    })
  })
})

test('md5 of utf-8 strings', function () {
  assert.equal(utils.md5('😊'), '5deda34cd95f304948d2bc1b4a62c11e')
})
