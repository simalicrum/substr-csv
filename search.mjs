export default ({ searchTerms, column, rows }) => {
  const found = [];
  for (const row of rows) {
    for (const term of searchTerms) {
      for (const prop of Object.values(row)) {
        if (prop.includes(term[column])) {
          found.push({ ...term, ...row });
        }
      }
    }
  }
  return found;
};
