const fs = require('fs');

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
  if (file.includes('replace_sonnet') || file.includes('restore_sonnet46')) return;
  let content = fs.readFileSync(file, 'utf8');
  let newContent = content
    .replace(/claude-3-7-sonnet-20250219/g, 'claude-sonnet-4-6')
    .replace(/Sonnet 3\.7/g, 'Sonnet 4.6')
    .replace(/sonnet 3\.7/g, 'sonnet 4.6')
    .replace(/sonnet-3\.7/g, 'sonnet-4.6')
    .replace(/sonnet-3-7/g, 'sonnet-4-6');
    
  if (content !== newContent) {
    fs.writeFileSync(file, newContent);
    console.log('Updated', file);
  }
});