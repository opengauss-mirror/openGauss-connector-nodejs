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
