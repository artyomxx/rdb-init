'use strict';

const fs = require('fs'),
	_stateFile = process.env.rdb_init_state_file || './.rethinkdb-init-state.json';

/* [
	{
		host: 'localhost',
		db: 'somedb',
		tables: [
			{
				name: 'tablename',
				indexes: ['index1', 'index2']
			},
			...
		]
	},
	...
] */

module.exports = (stateFile = _stateFile) => {
	let state = fs.existsSync(stateFile) ? JSON.parse(fs.readFileSync(stateFile, 'utf8')) || [] : [],
		lock = false;

	return {
		get: (dbname, table) => new Promise((resolve, reject) => {
			if(state.length) {
				let db = state.find(db => db.name === dbname);
				if(db.tables && db.tables.length) {
					resolve(db.tables.find(t => typeof table === 'string' ? table === t : table.name === t.name) || false)
				}
				else
					resolve(false);
			}
			else
				resolve(false);
		}),
		add: (dbname, table) => new Promise((resolve, reject) => {
			let i = setInterval(() => {
				if(lock) return;
				lock = true;

				if(state.length) {
					let dbIdx = state.findIndex(db => db.name === dbname);

					if(dbIdx === -1)
						state.push({name: dbname, tables: [table]});
					else {
						let tblIdx = Array.isArray(state[dbIdx].tables)
							? state[dbIdx].tables.findIndex(t => typeof table === 'string' ? table === t : table.name === t.name)
							: -1;

						if(!state[dbIdx].tables)
							state[dbIdx].tables = [];

						if(tblIdx === -1)
							state[dbIdx].tables.push(table);
					}
				}
				else
					state = [{name: dbname, tables: [table]}];

				lock = false;
				clearInterval(i);
				resolve(state);
			}, 5);
		}),
		save: () => new Promise((resolve, reject) =>
			fs.writeFile(stateFile, JSON.stringify(state), err => err ? reject(err) : resolve(state))
		)
	};
};
