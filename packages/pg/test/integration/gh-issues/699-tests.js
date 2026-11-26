/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/mit
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
