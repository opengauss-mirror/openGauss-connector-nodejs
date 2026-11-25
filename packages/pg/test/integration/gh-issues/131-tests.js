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

var suite = new helper.Suite()

suite.test('parsing array decimal results', function (done) {
  const pool = new pg.Pool()
  pool.connect(
    assert.calls(function (err, client, release) {
      assert(!err)
      client.query('CREATE TEMP TABLE why(names text[], numbors integer[], decimals double precision[])')
      client
        .query(
          new pg.Query(
            'INSERT INTO why(names, numbors, decimals) VALUES(\'{"aaron", "brian","a b c" }\', \'{1, 2, 3}\', \'{.1, 0.05, 3.654}\')'
          )
        )
        .on('error', console.log)
      client.query(
        'SELECT decimals FROM why',
        assert.success(function (result) {
          assert.lengthIs(result.rows[0].decimals, 3)
          assert.equal(result.rows[0].decimals[0], 0.1)
          assert.equal(result.rows[0].decimals[1], 0.05)
          assert.equal(result.rows[0].decimals[2], 3.654)
          release()
          pool.end(done)
        })
      )
    })
  )
})
