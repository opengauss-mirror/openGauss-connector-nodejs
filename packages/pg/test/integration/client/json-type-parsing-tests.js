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
var assert = require('assert')

const pool = new helper.pg.Pool()
pool.connect(
  assert.success(function (client, done) {
    helper.versionGTE(
      client,
      90200,
      assert.success(function (jsonSupported) {
        if (!jsonSupported) {
          console.log('skip json test on older versions of postgres')
          done()
          return pool.end()
        }
        client.query('CREATE TEMP TABLE stuff(id SERIAL PRIMARY KEY, data JSON)')
        var value = { name: 'Brian', age: 250, alive: true, now: new Date() }
        client.query('INSERT INTO stuff (data) VALUES ($1)', [value])
        client.query(
          'SELECT * FROM stuff',
          assert.success(function (result) {
            assert.equal(result.rows.length, 1)
            assert.equal(typeof result.rows[0].data, 'object')
            var row = result.rows[0].data
            assert.strictEqual(row.name, value.name)
            assert.strictEqual(row.age, value.age)
            assert.strictEqual(row.alive, value.alive)
            assert.equal(JSON.stringify(row.now), JSON.stringify(value.now))
            done()
            pool.end()
          })
        )
      })
    )
  })
)
