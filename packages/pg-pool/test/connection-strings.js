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

const expect = require('expect.js')
const describe = require('mocha').describe
const it = require('mocha').it
const Pool = require('../')

describe('Connection strings', function () {
  it('pool delegates connectionString property to client', function (done) {
    const connectionString = 'postgres://foo:bar@baz:1234/xur'

    const pool = new Pool({
      // use a fake client so we can check we're passed the connectionString
      Client: function (args) {
        expect(args.connectionString).to.equal(connectionString)
        return {
          connect: function (cb) {
            cb(new Error('testing'))
          },
          on: function () {},
        }
      },
      connectionString: connectionString,
    })

    pool.connect(function (err, client) {
      expect(err).to.not.be(undefined)
      done()
    })
  })
})
