const fs = require('fs');
const assert = require('assert');
const html = fs.readFileSync('index.html','utf8');
assert(html.includes('<title>Inspeção de Cana</title>'), 'title not found');
console.log('All tests passed');
