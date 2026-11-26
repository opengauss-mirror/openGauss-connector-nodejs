/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/mit
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
