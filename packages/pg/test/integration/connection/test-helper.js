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
var net = require('net')
var helper = require('../test-helper')
var Connection = require('../../../lib/connection')
var utils = require('../../../lib/utils')
var connect = function (callback) {
  var username = helper.args.user
  var database = helper.args.database
  var con = new Connection({ stream: new net.Stream() })
  con.on('error', function (error) {
    console.log(error)
    throw new Error('Connection error')
  })
  con.connect(helper.args.port || '5432', helper.args.host || 'localhost')
  con.once('connect', function () {
    con.startup({
      user: username,
      database: database,
    })
    con.once('authenticationCleartextPassword', function () {
      con.password(helper.args.password)
    })
    con.once('authenticationMD5Password', function (msg) {
      con.password(utils.postgresMd5PasswordHash(helper.args.user, helper.args.password, msg.salt))
    })
    con.once('readyForQuery', function () {
      con.query('create temp table ids(id integer)')
      con.once('readyForQuery', function () {
        con.query('insert into ids(id) values(1); insert into ids(id) values(2);')
        con.once('readyForQuery', function () {
          callback(con)
        })
      })
    })
  })
}

module.exports = {
  connect: connect,
}
