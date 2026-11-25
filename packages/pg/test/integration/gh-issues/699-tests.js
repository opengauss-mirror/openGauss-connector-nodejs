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
var copyFrom = require('pg-copy-streams').from

if (helper.args.native) return

const pool = new helper.pg.Pool()
pool.connect(function (err, client, done) {
  if (err) throw err

  var c = 'CREATE TEMP TABLE employee (id integer, fname varchar(400), lname varchar(400))'

  client.query(c, function (err) {
    if (err) throw err

    var stream = client.query(copyFrom('COPY employee FROM STDIN'))
    stream.on('end', function () {
      done()
      setTimeout(() => {
        pool.end()
      }, 50)
    })

    for (var i = 1; i <= 5; i++) {
      var line = ['1\ttest', i, '\tuser', i, '\n']
      stream.write(line.join(''))
    }
    stream.end()
  })
})
