const {DocumentClient, UriFactory} = require('documentdb')
const { Observable } = require('rxjs')

const connect = (config) => new Promise((resolve, reject) => {
  const client = new DocumentClient(config.host, {masterKey: config.key})
  const url = UriFactory.createDocumentCollectionUri(
    config.database,
    config.collection
  )
  resolve({
    client,
    url,
    close: () => {}
  })
})

const query = (connection, sql) => Observable.create((observer) => {
  const q = connection.client.queryDocuments(
    connection.url,
    sql,
    {enableCrossPartitionQuery: true}
  )
  var abort = false
  const poll = () =>
    q.nextItem((err, record) => {
      if (err) {
        observer.error(err)
      } else if (record) {
        observer.next(record)
        if (!abort) poll()
      } else {
        observer.complete()
      }
    })
  poll()
  return () => (abort = true)
})

const queryFactory = (config) => (line) => Observable.fromPromise(
  connect(config).then((conn) =>
    query(conn, line)
      .do(null, null, conn.close.bind(conn))
  )
).concatAll()

const createParameter = (name, value) => ({
  name,
  value,
  toString: () => typeof value === 'string' ? `'${value.split("'").join("''")}'`
    : value instanceof Date ? `'${value.toISOString()}'`
    : typeof value === 'object' ? JSON.stringify(value)
    : value === null ? 'NULL'
    : value === undefined ? 'NULL'
    : typeof value !== 'number' ? '[Unknown]'
    : value.toString()
})

const raw = (string) => new ''.constructor(string)

function sql (template) {
  var sqlParts = [template[0]]
  var stringParts = [template[0]]
  var parameters = []
  for (var i = 1; i < arguments.length; i++) {
    var pname = `@P${i}`
    var arg = arguments[i]
    if (typeof arg === 'object' && !(arg instanceof Date)) {
      sqlParts.push(arg, template[i])
      stringParts.push(arg, template[i])
    } else {
      var param = createParameter(pname, arguments[i])
      parameters.push(param)
      sqlParts.push(pname, template[i])
      stringParts.push(param, template[i])
    }
  }
  return {
    query: sqlParts.join(''),
    parameters,
    toString: () => stringParts.join('')
  }
}

module.exports = {
  connect, query, queryFactory, sql, raw
}
