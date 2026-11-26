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
// http://developer.postgresql.org/pgdocs/postgres/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

test('flushing once', function () {
  helper.connect(function (con) {
    con.parse({
      text: 'select * from ids',
    })

    con.bind()
    con.execute()
    con.flush()

    assert.emits(con, 'parseComplete')
    assert.emits(con, 'bindComplete')
    assert.emits(con, 'dataRow')
    assert.emits(con, 'commandComplete', function () {
      con.sync()
    })
    assert.emits(con, 'readyForQuery', function () {
      con.end()
    })
  })
})

test('sending many flushes', function () {
  helper.connect(function (con) {
    assert.emits(con, 'parseComplete', function () {
      con.bind()
      con.flush()
    })

    assert.emits(con, 'bindComplete', function () {
      con.execute()
      con.flush()
    })

    assert.emits(con, 'dataRow', function (msg) {
      assert.equal(msg.fields[0], 1)
      assert.emits(con, 'dataRow', function (msg) {
        assert.equal(msg.fields[0], 2)
        assert.emits(con, 'commandComplete', function () {
          con.sync()
        })
        assert.emits(con, 'readyForQuery', function () {
          con.end()
        })
      })
    })

    con.parse({
      text: 'select * from ids order by id',
    })

    con.flush()
  })
})
