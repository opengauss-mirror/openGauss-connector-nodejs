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
var helper = require('./../test-helper')

const suite = new helper.Suite()

helper.testPoolSize = function (max) {
  suite.test(`test ${max} queries executed on a pool rapidly`, (cb) => {
    const pool = new helper.pg.Pool({ max: 10 })

    var sink = new helper.Sink(max, function () {
      pool.end(cb)
    })

    for (var i = 0; i < max; i++) {
      pool.connect(function (err, client, done) {
        assert(!err)
        client.query('SELECT * FROM NOW()')
        client.query('select generate_series(0, 25)', function (err, result) {
          assert.equal(result.rows.length, 26)
        })
        var query = client.query('SELECT * FROM NOW()', (err) => {
          assert(!err)
          sink.add()
          done()
        })
      })
    }
  })
}

module.exports = Object.assign({}, helper, { suite: suite })
