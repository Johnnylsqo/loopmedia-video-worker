import express from "express"
import os from "os"
import fs from "fs"
import fsp from "fs/promises"
import path from "path"
import { pipeline } from "stream/promises"
import { spawn } from "child_process"
import { createClient } from "@supabase/supabase-js"

const {
  PORT = "3000",
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  SUPABASE_STORAGE_BUCKET,
  WORKER_ID = `worker-${os.hostname()}`,
  POLL_INTERVAL_MS = "5000",
  TEMP_DIR = "/tmp/loopmedia"
} = process.env

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  { auth: { persistSession: false } }
)

const app = express()

app.get("/health", (_, res) => {
  res.json({
    ok: true,
    worker: WORKER_ID,
    time: new Date().toISOString()
  })
})

app.listen(PORT, () => {
  console.log("Worker health server running")
})

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function run(cmd, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args)
    p.stderr.on("data", d => {
      console.log(d.toString())
    })
    p.on("close", code => {
      if (code === 0) resolve()
      else reject(new Error(`${cmd} exited ${code}`))
    })
  })
}

async function downloadFile(storagePath, localPath) {
  const { data } = await supabase
    .storage
    .from(SUPABASE_STORAGE_BUCKET)
    .createSignedUrl(storagePath, 3600)

  const res = await fetch(data.signedUrl)
  const stream = fs.createWriteStream(localPath)
  await pipeline(res.body, stream)
  const stat = await fsp.stat(localPath)
  return stat.size
}

async function uploadFile(localPath, outputPath) {
  const stream = fs.createReadStream(localPath)
  const { error } = await supabase
    .storage
    .from(SUPABASE_STORAGE_BUCKET)
    .upload(outputPath, stream, {
      contentType: "video/mp4",
      upsert: true
    })
  if (error) throw error
  const stat = await fsp.stat(localPath)
  return stat.size
}

async function transcode(input, output) {
  console.log("Starting ffmpeg")
  await run("ffmpeg", [
    "-y",
    "-i", input,
    "-c:v", "libx264",
    "-preset", "ultrafast",
    "-crf", "30",
    "-pix_fmt", "yuv420p",
    "-movflags", "+faststart",
    "-c:a", "aac",
    "-b:a", "128k",
    output
  ])
  console.log("ffmpeg done")
}

async function claimJob() {
  const { data, error } = await supabase.rpc("claim_video_job", {
    worker_name: WORKER_ID
  })
  if (error) throw error
  if (!data) return null
  return Array.isArray(data) ? data[0] : data
}

async function failJob(id, message) {
  await supabase.rpc("fail_video_job", {
    job_id: id,
    error: message
  })
}

async function completeJob(id, outputPath, size) {
  await supabase.rpc("complete_video_job", {
    job_id: id,
    output_path: outputPath,
    output_size: size
  })
}

async function processJob(job) {
  const jobId = job.id
  console.log("Processing job", jobId)
  const tempDir = path.join(TEMP_DIR, jobId)
  await fsp.mkdir(tempDir, { recursive: true })
  const input = path.join(tempDir, "input")
  const output = path.join(tempDir, "output.mp4")
  try {
    await downloadFile(job.original_path, input)
    await transcode(input, output)
    const outputPath = `processed/${jobId}.mp4`
    const size = await uploadFile(output, outputPath)
    await completeJob(jobId, outputPath, size)
  } catch (err) {
    console.error("Job failed", err)
    await failJob(jobId, err.message)
  }
  await fsp.rm(tempDir, { recursive: true, force: true })
}

async function workerLoop() {
  await fsp.mkdir(TEMP_DIR, { recursive: true })
  console.log("Worker started")
  while (true) {
    try {
      const job = await claimJob()
      if (!job) {
        await sleep(POLL_INTERVAL_MS)
        continue
      }
      await processJob(job)
    } catch (err) {
      console.error("Worker error", err)
      await sleep(POLL_INTERVAL_MS)
    }
  }
}

workerLoop()