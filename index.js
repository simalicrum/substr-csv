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
  // The URL must be a file:// URL
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

fs.writeFileSync(options.csvOutputFile, "");

const glob = new Glob(options.csvInputFiles, { nodir: true });
let foundcount = 0;
let threadnum = 0;
let cpus = os.cpus().length;
let threads = Array(cpus).fill([]);
let promises = [];
for await (const filename of glob) {
  spinner.start(`Searching ${filename}`);
  await new Promise((resolve, reject) => {
    const file = fs.createReadStream(filename);
    Papa.parse(file, {
      worker: true,
      header: true,
      skipEmptyLines: true,
      download: true,
      complete: () => {
        spinner.suffixText = "";
        spinner.succeed();
        resolve();
      },
      step: (row) => {
        threads[threadnum++].push(row);
        if (threadnum === cpus) {
          threadnum = 0;
        }
      },
      error: (error) => {
        console.log("error: ", error);
        reject(error);
      },
    });
  });
  for (const thread of threads) {
    const promise = piscina.run({
      searchTerms: searchTerms.data,
      column: options.column,
      rows: thread,
    });
    promises.push(promise);
  }
  const foundChunks = await Promise.all(promises);
  const found = foundChunks.flat();
  foundcount += found.length;
  threads = Array(cpus).fill([]);
  promises = [];
  const str = Papa.unparse(found, { header: false });
  appendFile(options.csvOutputFile, str, "utf8");
  if (foundcount) {
    spinner.suffixText = chalk.dim(`${foundcount} matches found`);
  }
}

console.log("Done");
