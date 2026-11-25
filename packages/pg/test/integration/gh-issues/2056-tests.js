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
var helper = require('./../test-helper')
var assert = require('assert')

const suite = new helper.Suite()

suite.test('All queries should return a result array', (done) => {
  const client = new helper.pg.Client()
  client.connect()
  const promises = []
  promises.push(client.query('CREATE TEMP TABLE foo(bar TEXT)'))
  promises.push(client.query('INSERT INTO foo(bar) VALUES($1)', ['qux']))
  promises.push(client.query('SELECT * FROM foo WHERE bar = $1', ['foo']))
  Promise.all(promises).then((results) => {
    results.forEach((res) => {
      assert(Array.isArray(res.fields))
      assert(Array.isArray(res.rows))
    })
    client.end(done)
  })
})
