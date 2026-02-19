const fs = require('fs');
const glob = require('glob'); // Not available? We can just use basic recursion.

const walk = (dir) => {
  let results = [];
  const list = fs.readdirSync(dir);
  list.forEach((file) => {
    file = dir + '/' + file;
    const stat = fs.statSync(file);
    if (stat && stat.isDirectory()) { 
      if (!file.includes('node_modules') && !file.includes('.git') && !file.includes('data')) {
        results = results.concat(walk(file));
      }
    } else { 
      if (file.endsWith('.js') || file.endsWith('.html') || file.endsWith('.md')) {
        results.push(file);
      }
    }
  });
  return results;
};

const files = walk('.');

files.forEach(file => {
  let content = fs.readFileSync(file, 'utf8');
  let newContent = content
    .replace(/Opus 4\.6/g, 'Sonnet 4.6')
    .replace(/opus 4\.6/g, 'sonnet 4.6')
    .replace(/Opus/g, 'Sonnet')
    .replace(/opus/g, 'sonnet')
    .replace(/OPUS/g, 'SONNET');
    
  if (content !== newContent) {
    fs.writeFileSync(file, newContent);
    console.log('Updated', file);
  }
});
