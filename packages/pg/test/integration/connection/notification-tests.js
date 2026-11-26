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
// http://www.postgresql.org/docs/8.3/static/libpq-notify.html
test('recieves notification from same connection with no payload', function () {
  helper.connect(function (con) {
    con.query('LISTEN boom')
    assert.emits(con, 'readyForQuery', function () {
      con.query('NOTIFY boom')
      assert.emits(con, 'notification', function (msg) {
        assert.equal(msg.payload, '')
        assert.equal(msg.channel, 'boom')
        con.end()
      })
    })
  })
})
