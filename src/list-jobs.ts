import { initBqClient } from "./init-bq-client";
import * as dotenv from "dotenv";
dotenv.config();

async function main() {
  const bqClient = await initBqClient();
  console.log("Listing all jobs...");
  const [jobs, options, metadata] = await bqClient.getJobs();
  console.log(`Jobs (${jobs.length}) fetched successfully !`);
  return { jobs, options, metadata };
}

main().then((result) => console.log("DONE !", result));
