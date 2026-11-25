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
const pool = new helper.pg.Pool()

pool.connect(
  assert.success(function (client, done) {
    var types = require('pg-types')
    // 1231 = numericOID
    types.setTypeParser(1700, function () {
      return 'yes'
    })
    types.setTypeParser(1700, 'binary', function () {
      return 'yes'
    })
    var bignum = '294733346389144765940638005275322203805'
    client.query('CREATE TEMP TABLE bignumz(id numeric)')
    client.query('INSERT INTO bignumz(id) VALUES ($1)', [bignum])
    client.query(
      'SELECT * FROM bignumz',
      assert.success(function (result) {
        assert.equal(result.rows[0].id, 'yes')
        done()
        pool.end()
      })
    )
  })
)
