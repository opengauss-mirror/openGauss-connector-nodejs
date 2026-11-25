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
var pg = helper.pg
const suite = new helper.Suite()

const pool = new pg.Pool(helper.config)
suite.test('ability to turn on and off parser', function () {
  if (helper.args.binary) return false
  pool.connect(
    assert.success(function (client, done) {
      pg.defaults.parseInt8 = true
      client.query('CREATE TEMP TABLE asdf(id SERIAL PRIMARY KEY)')
      client.query(
        'SELECT COUNT(*) as "count", \'{1,2,3}\'::bigint[] as array FROM asdf',
        assert.success(function (res) {
          assert.strictEqual(0, res.rows[0].count)
          assert.strictEqual(1, res.rows[0].array[0])
          assert.strictEqual(2, res.rows[0].array[1])
          assert.strictEqual(3, res.rows[0].array[2])
          pg.defaults.parseInt8 = false
          client.query(
            'SELECT COUNT(*) as "count", \'{1,2,3}\'::bigint[] as array FROM asdf',
            assert.success(function (res) {
              done()
              assert.strictEqual('0', res.rows[0].count)
              assert.strictEqual('1', res.rows[0].array[0])
              assert.strictEqual('2', res.rows[0].array[1])
              assert.strictEqual('3', res.rows[0].array[2])
              pool.end()
            })
          )
        })
      )
    })
  )
})
