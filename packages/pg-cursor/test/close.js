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

const assert = require('assert')
const Cursor = require('../')
const pg = require('pg')

const text = 'SELECT generate_series as num FROM generate_series(0, 50)'
describe('close', function () {
  beforeEach(function (done) {
    const client = (this.client = new pg.Client())
    client.connect(done)
  })

  this.afterEach(function (done) {
    this.client.end(done)
  })

  it('can close a finished cursor without a callback', function (done) {
    const cursor = new Cursor(text)
    this.client.query(cursor)
    this.client.query('SELECT NOW()', done)
    cursor.read(100, function (err) {
      assert.ifError(err)
      cursor.close()
    })
  })

  it('closes cursor early', function (done) {
    const cursor = new Cursor(text)
    this.client.query(cursor)
    this.client.query('SELECT NOW()', done)
    cursor.read(25, function (err) {
      assert.ifError(err)
      cursor.close()
    })
  })

  it('works with callback style', function (done) {
    const cursor = new Cursor(text)
    const client = this.client
    client.query(cursor)
    cursor.read(25, function (err, rows) {
      assert.ifError(err)
      assert.strictEqual(rows.length, 25)
      cursor.close(function (err) {
        assert.ifError(err)
        client.query('SELECT NOW()', done)
      })
    })
  })

  it('is a no-op to "close" the cursor before submitting it', function (done) {
    const cursor = new Cursor(text)
    cursor.close(done)
  })
})
