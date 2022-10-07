import { BigQuery } from "@google-cloud/bigquery";

enum BigQueryScopes {
  BIGQUERY = "https://www.googleapis.com/auth/bigquery",
}

const BIGQUERY_SCOPES = Object.values(BigQueryScopes);

export async function initBqClient() {
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

  return bigQueryClient;
}
