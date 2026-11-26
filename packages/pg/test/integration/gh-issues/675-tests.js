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
var helper = require('../test-helper')
var assert = require('assert')

const pool = new helper.pg.Pool()
pool.connect(function (err, client, done) {
  if (err) throw err

  var c = 'CREATE TEMP TABLE posts (body TEXT)'

  client.query(c, function (err) {
    if (err) throw err

    c = 'INSERT INTO posts (body) VALUES ($1) RETURNING *'

    var body = Buffer.from('foo')
    client.query(c, [body], function (err) {
      if (err) throw err

      body = Buffer.from([])
      client.query(c, [body], function (err, res) {
        done()

        if (err) throw err
        assert.equal(res.rows[0].body, '')
        pool.end()
      })
    })
  })
})
