'use strict';

module.exports = (r, stateFilePath) => {
	const st = require('./state')(stateFilePath),
		pick = (obj, fields) => fields.reduce((o, f) => obj[f] ? Object.assign(o, {[f]: obj[f]}) : o, {});

	if(typeof r !== 'function')
		throw new TypeError(`r must be a RethinkDB instance. Passed instances is a '${typeof r}'`);

	const isObject = v => typeof v === 'object' && v !== null && !Array.isArray(v);

	const existsHandler = err => {
		if(err.name in {ReqlOpFailedError: 0, RqlRuntimeError: 0} && (err.msg || err.message).includes('already exists'))
			return true;
		else
			throw err;
	};

	const createIndexes = (table, db, conn) => new Promise((resolve, reject) => Promise.all(
		table.indexes.map(index => new Promise((resolve, reject) => {
			if(!index.name && typeof index !== 'string')
				throw new TypeError(`Index entry in table (${table.name}) entry must be 'Object' (with '.name' property) or 'String'`);

			if(typeof index === 'string')
				r.db(db).table(table.name).indexCreate(index).run(conn).catch(existsHandler).then(resolve);
			else
				r.db(db).table(table.name)
				.indexCreate(index.name, index.indexFunction || pick(index, ['multi', 'geo']), index.indexFunction ? pick(index, ['multi', 'geo']) : undefined)
				.run(conn)
				.catch(existsHandler)
				.then(resolve);
		}))
	).then(resolve, reject));

	const createTables = (tables, db, conn) => new Promise((resolve, reject) => Promise.all(
		tables.map(table => new Promise((resolve, reject) => {
			if(!table.name && typeof table !== 'string')
				throw new TypeError('Table entry in table entry must be `Object` (with `.name` property) or `String`');

			st.get(db, table)
			.then(state => {
				if(state)
					return resolve(console.log(`table ${db}.${table.name || table} already exists`));

				if(typeof table === 'string')
					r.db(db).tableCreate(table).run(conn)
					.catch(existsHandler)
					.then(() =>
						st.add(db, table)
						.then(() => resolve(console.log(`created table ${db}.${table}`)))
						.catch(() => { throw new Error(`Couldn't add created table ${db}.${table} to the state file`); })
					);
				else
					r.db(db)
					.tableCreate(table.name, pick(table, ['primaryKey', 'durability', 'shards', 'replicas', 'primaryReplicaTag']))
					.run(conn)
					.catch(existsHandler)
					.then(() =>
						st.add(db, table)
						.then(() => {
							if(table.indexes && table.indexes.length)
								createIndexes(table, db, conn)
								.then(() => resolve(console.log(`created table ${db}.${table.name} with ${table.indexes.length} indexes`)))
								.catch(() => resolve(console.log(`created table ${db}.${table.name}, couldn't create ${table.indexes.length} indexes`)));
							else
								resolve(console.log(`created table ${db}.${table.name}`));
						})
					);
			})
			.catch(() => { throw new Error('Something is wrong with state data.'); });
		}))
	).then(resolve, reject));

	r.init = (connection, schema) => new Promise((resolve, reject) => {
		if(typeof connection.db !== 'string')
			throw new TypeError('Connection is not an object or doesn`t have a db property.');

		if(!Array.isArray(schema))
			throw new TypeError('Schema must be an array.');

		return r.connect(connection)
			.then(conn => r.dbCreate(conn.db).run(conn)
				.catch(existsHandler)
				.then(() => createTables(schema, connection.db, conn)
					.then(() =>
						st.save()
						.then(() => resolve(conn))
						.catch(reject)
					)
					.catch(reject)
				)
			);
	});

	return r.init;
};
