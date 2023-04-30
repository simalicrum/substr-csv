import fs from "fs";
import util from "util";
import Papa from "papaparse";
import ora from "ora";
import chalk from "chalk";
import { Command } from "commander";
import { Glob } from "glob";
import { Piscina } from "piscina";
import os from "os";

const piscina = new Piscina({
  filename: new URL("./search.mjs", import.meta.url).href,
});

export const sleep = (delay) =>
  new Promise((resolve) => setTimeout(resolve, delay));

const program = new Command();

program
  .requiredOption(
    "-i, --csv-input-files <file-pattern>",
    "File pattern of CSV files to search within for the search terms specified by --csv-search-terms"
  )
  .requiredOption(
    "-s, --csv-search-terms <file>",
    "CSV file containing a column of search terms to search within the CSV files specified by --csv-input-files"
  )
  .option("-o, --csv-output-file <file>", "CSV file to output the results to")
  .requiredOption(
    "-c, --column <column>",
    "Column to search within the search terms CSV"
  );

program.parse();

const options = program.opts();

const spinner = ora(
  `Loading search terms from ${chalk.green(options.csvSearchTerms)}`
).start();

try {
  const searchTermsCsv = fs.readFileSync(options.csvSearchTerms, "utf8");
  var searchTerms = Papa.parse(searchTermsCsv, {
    header: true,
    skipEmptyLines: true,
  });
} catch (error) {
  spinner.fail();
  console.error(error);
  process.exit(1);
}

spinner.succeed();

console.log(
  `Searching the data set for ${chalk.green(searchTerms.data.length)} terms`
);

const appendFile = util.promisify(fs.appendFile);
const readFile = util.promisify(fs.readFile);

fs.writeFileSync(options.csvOutputFile, "");

const glob = new Glob(options.csvInputFiles, { nodir: true });
let foundcount = 0;
let cpus = os.cpus().length;
let promises = [];
for await (const filename of glob) {
  spinner.start(`Searching ${filename}`);
  const csv = await readFile(filename, "utf8");
  const rows = Papa.parse(csv, {
    header: true,
    skipEmptyLines: true,
  });
  const promise = piscina.run({
    searchTerms: searchTerms.data,
    column: options.column,
    rows: rows.data,
  });
  promises.push(promise);
  if (promises.length >= cpus) {
    const found = await Promise.all(promises);
    found.forEach((chunk) => {
      foundcount += chunk.length;
      if (chunk.length > 0) {
        const str = Papa.unparse(chunk);
        appendFile(options.csvOutputFile, str);
      }
    });
    promises = [];
  }
  if (foundcount) {
    spinner.suffixText = chalk.dim(`Found ${foundcount} matches`);
  }
  spinner.succeed();
}
console.log("Done");
