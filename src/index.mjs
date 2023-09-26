import { S3Client, SelectObjectContentCommand } from "@aws-sdk/client-s3";

const client = new S3Client({ region: "us-east-1" });

const comercialStructureId = "7170";
const resellerId = "560";

const params = {
  Bucket: "vd-unified-bff",
  Key: "segmentation/560.csv",
  ExpressionType: "SQL",
  Expression: `SELECT s._1 as response FROM s3object s WHERE s._1 like '${resellerId}' or s._1 like '${comercialStructureId}'`,
  InputSerialization: {
    CSV: { FileHeaderInfo: "NONE" },
    CompressionType: "NONE",
  },
  OutputSerialization: {
    JSON: {},
  },
};

const command = new SelectObjectContentCommand(params);

const response = await client.send(command);
let chunks = [];
for await (const item of response.Payload) {
  if (typeof item.Records !== "undefined") {
    chunks.push(item.Records.Payload);
  }
}

console.log("RESULT", Buffer.concat(chunks).toString("utf-8"));
