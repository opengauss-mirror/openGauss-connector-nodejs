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

import assert from 'assert'
import helper from './helper'
import QueryStream from '../src'

helper('passing options', function (client) {
  it('passes row mode array', function (done) {
    const stream = new QueryStream('SELECT * FROM generate_series(0, 10) num', [], { rowMode: 'array' })
    const query = client.query(stream)
    const result = []
    query.on('data', (datum) => {
      result.push(datum)
    })
    query.on('end', () => {
      const expected = new Array(11).fill(0).map((_, i) => [i])
      assert.deepEqual(result, expected)
      done()
    })
  })

  it('passes custom types', function (done) {
    const types = {
      getTypeParser: () => (string) => string,
    }
    const stream = new QueryStream('SELECT * FROM generate_series(0, 10) num', [], { types })
    const query = client.query(stream)
    const result = []
    query.on('data', (datum) => {
      result.push(datum)
    })
    query.on('end', () => {
      const expected = new Array(11).fill(0).map((_, i) => ({
        num: i.toString(),
      }))
      assert.deepEqual(result, expected)
      done()
    })
  })
})
