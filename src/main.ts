import { BigQuery, Job, JobMetadata } from "@google-cloud/bigquery";
import pWaitFor from "p-wait-for";
import * as dotenv from "dotenv";

dotenv.config();

enum BigQueryScopes {
  BIGQUERY = "https://www.googleapis.com/auth/bigquery",
}

const BIGQUERY_SCOPES = Object.values(BigQueryScopes);

/**
 * User made strategy to load csv file in table that might not exist and auto-detect structure
 */
const LOAD_AUTO_CREATE_AND_APPEND_STRATEGY = {
  autodetect: true,
  createDisposition: "CREATE_IF_NEEDED",
  writeDisposition: "WRITE_APPEND",
  schemaUpdateOptions: ["ALLOW_FIELD_ADDITION"],
};
async function main() {
  /**
   * Init
   */
  const keyFilename = process.env.BIGQUERY_CLIENT_SECRETS;
  const projectId = process.env.BIGQUERY_PROJECT_ID;
  const bigQueryClient = new BigQuery({
    keyFilename,
    projectId,
    scopes: BIGQUERY_SCOPES,
  });

  /**
   * Auth using client
   */
  console.debug(`Authenticated to BigQuery for project '${projectId}'`);
  const bqClientProjectId = await bigQueryClient.getProjectId();
  if (!Boolean(bqClientProjectId)) {
    throw new Error("Unable to get project id from bigquery");
  }

  /**
   * Prepare job params
   * Insert your values inside of quotes
   * Note: Dataset (and table depending on job metadata provided) must exist
   */
  const datasetId = process.env.BIGQUERY_DATASET_ID || "YOUR_DATASET_ID";
  const tableId = process.env.BIGQUERY_TABLE_ID || "YOUR_TABLE_ID";
  const file = process.env.BIGQUERY_FILE_TO_LOAD || "path/to/your/csv/file";
  const jobLoadMetadata = LOAD_AUTO_CREATE_AND_APPEND_STRATEGY; // Replace metadata with your own 😉

  try {
    /**
     * Create load job
     */
    console.log("Create load job...");
    const dataset = bigQueryClient.dataset(datasetId);
    const table = dataset.table(tableId);
    const [loadJob, jobMetadata] = await table.createLoadJob(
      file,
      jobLoadMetadata
    );
    console.log(`Load job created successfully ! Job id: ${loadJob.id}`);
    /**
     * Wait for job to finish
     */
    await pWaitFor(() => pollJob(loadJob), {
      before: false, // Do not run check immediately
      interval: 10_000, // 10 seconds
      timeout: 600_000, // 10 minutes,
    });

    return true;
  } catch (e) {
    console.error("Error occured while creating or polling job", e);
    return false;
  }
}

async function pollJob(job: Job): Promise<boolean | never> {
  console.log(`Polling job: ${job.id}...`);
  const [metadata] = (await job.getMetadata()) as [JobMetadata, any];

  if (metadata.status?.errorResult) {
    throw new Error(
      `Bigquery job error: ${JSON.stringify(metadata.status?.errorResult)}`
    );
  }

  return metadata.status?.state === "DONE";
}

main().then(() => console.log("DONE"));
