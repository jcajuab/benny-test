import { UnstructuredClient } from "unstructured-client";
import { type PartitionResponse } from "unstructured-client/sdk/models/operations";
import { Strategy } from "unstructured-client/sdk/models/shared";
import fs from "node:fs";
import { openai } from "@ai-sdk/openai";
import { embedMany } from "ai";
import { QdrantClient } from "@qdrant/js-client-rest";

// Initialize clients
const client = new UnstructuredClient({
  serverURL: process.env.UNSTRUCTURED_API_URL!,
  security: {
    apiKeyAuth: process.env.UNSTRUCTURED_API_KEY!,
  },
});

const qdrantClient = new QdrantClient({
  url: process.env.QDRANT_API_URL!,
  apiKey: process.env.QDRANT_API_KEY!,
  port: null, // Don't append default port since URL already includes routing
});

const COLLECTION_NAME = "document_embeddings";
const filename = "sample.docx";

async function main() {
  console.log("=== DOCUMENT PROCESSING PIPELINE ===\n");

  // STEP 1: DOCUMENT PREPROCESSING VIA UNSTRUCTURED
  console.log("[STEP 1/3] DOCUMENT PREPROCESSING");
  console.log(`- Reading file: ${filename}`);

  let data: Buffer;
  try {
    data = fs.readFileSync(filename);
    console.log(`✓ File read successfully (${data.length} bytes)`);
  } catch (error) {
    console.error("✗ Error reading file:", error);
    throw error;
  }

  console.log(`- Calling Unstructured API for document partitioning...`);
  let elements: Array<{ [k: string]: any }>;

  try {
    const response: PartitionResponse = await client.general.partition({
      partitionParameters: {
        files: {
          content: data,
          fileName: filename,
        },
        strategy: Strategy.HiRes,
        splitPdfPage: true,
        splitPdfAllowFailed: true,
        splitPdfConcurrencyLevel: 15,
        languages: ["eng"],
      },
    });

    elements = Array.isArray(response) ? response : [];
    console.log(`✓ Document partitioned successfully`);
    console.log(`  - Total elements: ${elements.length}`);
  } catch (error) {
    console.error("✗ Error during document partitioning:");
    console.error("  Error details:", error);
    throw error;
  }

  if (elements.length === 0) {
    console.log("⚠ No elements found in partition response. Exiting.");
    return;
  }

  // Extract text from elements
  console.log(`- Extracting text from elements...`);
  let texts: string[];

  try {
    texts = elements
      .map((element: any) => element.text)
      .filter((text: string) => text && text.trim().length > 0);

    console.log(`✓ Text extraction complete`);
    console.log(`  - Total text chunks: ${texts.length}`);
    const firstText = texts[0];
    if (firstText) {
      console.log(
        `  - Sample text (first chunk): "${firstText.substring(0, 100)}${firstText.length > 100 ? "..." : ""}"`,
      );
    }
  } catch (error) {
    console.error("✗ Error extracting text from elements:");
    console.error("  Error details:", error);
    throw error;
  }

  if (texts.length === 0) {
    console.log("⚠ No text content found in elements. Exiting.");
    return;
  }

  // STEP 2: EMBEDDING GENERATION VIA OPENAI API
  console.log("\n[STEP 2/3] EMBEDDING GENERATION");
  console.log(`- Generating embeddings for ${texts.length} text chunks...`);
  console.log(`  - Model: text-embedding-3-small`);

  let embeddings: number[][];

  try {
    const result = await embedMany({
      model: openai.embedding("text-embedding-3-small"),
      values: texts,
    });

    embeddings = result.embeddings as number[][];
    console.log(`✓ Embeddings generated successfully`);
    console.log(`  - Total embeddings: ${embeddings.length}`);
    const firstEmbedding = embeddings[0];
    if (firstEmbedding) {
      console.log(`  - Embedding dimension: ${firstEmbedding.length}`);
      console.log(
        `  - Sample embedding (first 5 dims): [${firstEmbedding.slice(0, 5).join(", ")}...]`,
      );
    }
  } catch (error) {
    console.error("✗ Error generating embeddings:");
    console.error("  Error details:", error);
    throw error;
  }

  // STEP 3: EMBEDDING PERSISTENCE VIA QDRANT
  console.log("\n[STEP 3/3] EMBEDDING PERSISTENCE");
  console.log(`- Connecting to Qdrant at ${process.env.QDRANT_API_URL}`);

  const vectorSize = embeddings[0]?.length ?? 1536;

  // Ensure collection exists
  try {
    console.log(`- Checking if collection '${COLLECTION_NAME}' exists...`);
    await qdrantClient.getCollection(COLLECTION_NAME);
    console.log(`✓ Collection '${COLLECTION_NAME}' already exists`);
  } catch (error) {
    console.log(`- Collection not found, creating '${COLLECTION_NAME}'...`);

    try {
      await qdrantClient.createCollection(COLLECTION_NAME, {
        vectors: {
          size: vectorSize,
          distance: "Cosine",
        },
      });
      console.log(`✓ Collection '${COLLECTION_NAME}' created successfully`);
      console.log(`  - Vector size: ${vectorSize}`);
      console.log(`  - Distance metric: Cosine`);
    } catch (createError) {
      console.error("✗ Error creating Qdrant collection:");
      console.error("  Error details:", createError);
      throw createError;
    }
  }

  // Prepare points for Qdrant
  console.log(`- Preparing ${texts.length} points for insertion...`);
  let points;

  try {
    points = texts.map((text: string, idx: number) => ({
      id: idx,
      vector: embeddings[idx]!,
      payload: {
        text,
        filename,
        element_index: idx,
        element_metadata: elements[idx],
      },
    }));
    console.log(`✓ Points prepared successfully`);
  } catch (error) {
    console.error("✗ Error preparing points:");
    console.error("  Error details:", error);
    throw error;
  }

  // Upsert points to Qdrant
  console.log(`- Upserting points to collection '${COLLECTION_NAME}'...`);

  try {
    await qdrantClient.upsert(COLLECTION_NAME, {
      wait: true,
      points,
    });
    console.log(`✓ Successfully stored ${points.length} embeddings in Qdrant`);
  } catch (error) {
    console.error("✗ Error upserting to Qdrant:");
    console.error("  Error details:", error);
    throw error;
  }

  console.log("\n=== PIPELINE COMPLETED SUCCESSFULLY ===");
  console.log(`Summary:`);
  console.log(`  - Document: ${filename}`);
  console.log(`  - Elements processed: ${elements.length}`);
  console.log(`  - Text chunks: ${texts.length}`);
  console.log(`  - Embeddings generated: ${embeddings.length}`);
  console.log(`  - Vectors stored in Qdrant: ${points.length}`);
  console.log(`  - Collection: ${COLLECTION_NAME}`);
}

// Execute main function with error handling
main().catch((error) => {
  console.error("\n=== PIPELINE FAILED ===");
  console.error("Fatal error occurred:");
  console.error(error);
  process.exit(1);
});
