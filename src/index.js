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
  TEMP_DIR = "/tmp/loopmedia",
  FFMPEG_CRF = "30",
  FFMPEG_PRESET = "ultrafast",
  MAX_JOB_ATTEMPTS = "3",
  MAX_INPUT_SIZE_BYTES = "2147483648",
  MAX_OUTPUT_WIDTH = "1920",
  MAX_OUTPUT_HEIGHT = "1080",
  STALE_REQUEUE_MINUTES = "30",
  REQUEUE_CHECK_EVERY_LOOPS = "12"
} = process.env

function validateEnv() {
  const required = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_STORAGE_BUCKET"
  ]

  const missing = required.filter((key) => !process.env[key])

  if (missing.length > 0) {
    console.error("[startup] Missing required env vars:", missing.join(", "))
    process.exit(1)
  }
}

validateEnv()

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false }
})

const app = express()

app.get("/health", (_req, res) => {
  res.status(200).json({
    ok: true,
    worker: WORKER_ID,
    bucket: SUPABASE_STORAGE_BUCKET,
    uptimeSeconds: Math.floor(process.uptime()),
    time: new Date().toISOString()
  })
})

app.listen(Number(PORT), () => {
  console.log(`[http] Worker health server running on port ${PORT}`)
})

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function ensureDir(dirPath) {
  await fsp.mkdir(dirPath, { recursive: true })
}

function safeString(value) {
  return String(value || "").trim()
}

function runProcess(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ["ignore", "pipe", "pipe"],
      ...options
    })

    let stdout = ""
    let stderr = ""

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString()
    })

    child.stderr.on("data", (chunk) => {
      const text = chunk.toString()
      stderr += text
      process.stdout.write(`[${command}] ${text}`)
    })

    child.on("error", (err) => {
      reject(err)
    })

    child.on("close", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr })
        return
      }

      reject(new Error(`${command} exited with code ${code}\n${stderr || stdout}`))
    })
  })
}

async function ffprobeFile(inputPath) {
  console.log(`[ffprobe] start ${inputPath}`)

  const args = [
    "-v", "error",
    "-print_format", "json",
    "-show_streams",
    "-show_format",
    inputPath
  ]

  const { stdout } = await runProcess("ffprobe", args)
  const parsed = JSON.parse(stdout)

  const videoStream = (parsed.streams || []).find((s) => s.codec_type === "video")
  const audioStream = (parsed.streams || []).find((s) => s.codec_type === "audio")
  const format = parsed.format || {}

  const metadata = {
    durationSeconds: format.duration ? Math.round(Number(format.duration)) : null,
    width: videoStream?.width ?? null,
    height: videoStream?.height ?? null,
    videoCodec: safeString(videoStream?.codec_name).toLowerCase(),
    audioCodec: safeString(audioStream?.codec_name).toLowerCase(),
    pixelFormat: safeString(videoStream?.pix_fmt).toLowerCase(),
    formatName: safeString(format.format_name).toLowerCase(),
    bitRate: format.bit_rate ? Number(format.bit_rate) : null
  }

  console.log(`[ffprobe] done ${JSON.stringify(metadata)}`)
  return metadata
}

async function downloadFromStorageToFile(storagePath, destinationPath) {
  console.log(`[download] start bucket=${SUPABASE_STORAGE_BUCKET} path=${storagePath}`)

  const { data, error } = await supabase
    .storage
    .from(SUPABASE_STORAGE_BUCKET)
    .createSignedUrl(storagePath, 3600)

  if (error) {
    throw new Error(
      `createSignedUrl failed for bucket="${SUPABASE_STORAGE_BUCKET}" path="${storagePath}": ${error.message}`
    )
  }

  if (!data || !data.signedUrl) {
    throw new Error(
      `createSignedUrl returned no signedUrl for bucket="${SUPABASE_STORAGE_BUCKET}" path="${storagePath}"`
    )
  }

  const response = await fetch(data.signedUrl)

  if (!response.ok) {
    throw new Error(
      `download failed for bucket="${SUPABASE_STORAGE_BUCKET}" path="${storagePath}" with status ${response.status}`
    )
  }

  if (!response.body) {
    throw new Error(
      `download returned empty body for bucket="${SUPABASE_STORAGE_BUCKET}" path="${storagePath}"`
    )
  }

  const writeStream = fs.createWriteStream(destinationPath)
  await pipeline(response.body, writeStream)

  const stats = await fsp.stat(destinationPath)
  console.log(`[download] done ${destinationPath} (${stats.size} bytes)`)

  return stats.size
}

async function uploadFileToStorage(localPath, outputPath) {
  console.log(`[upload] start local=${localPath} output=${outputPath}`)

  const readStream = fs.createReadStream(localPath)

  const { error } = await supabase
    .storage
    .from(SUPABASE_STORAGE_BUCKET)
    .upload(outputPath, readStream, {
      contentType: "video/mp4",
      upsert: true
    })

  if (error) {
    throw new Error(`upload failed for output="${outputPath}": ${error.message}`)
  }

  const stats = await fsp.stat(localPath)
  console.log(`[upload] done ${outputPath} (${stats.size} bytes)`)

  return stats.size
}

function isAlreadyCompatible(metadata) {
  const maxWidth = Number(MAX_OUTPUT_WIDTH)
  const maxHeight = Number(MAX_OUTPUT_HEIGHT)

  const isMp4 = metadata.formatName.includes("mp4")
  const isH264 = metadata.videoCodec === "h264"
  const isAacOrNoAudio = metadata.audioCodec === "aac" || metadata.audioCodec === ""
  const isYuv420p = metadata.pixelFormat === "yuv420p" || metadata.pixelFormat === ""
  const withinWidth = !metadata.width || metadata.width <= maxWidth
  const withinHeight = !metadata.height || metadata.height <= maxHeight

  return (
    isMp4 &&
    isH264 &&
    isAacOrNoAudio &&
    isYuv420p &&
    withinWidth &&
    withinHeight
  )
}

async function copyCompatibleVideo(inputPath, outputPath) {
  console.log(`[copy] video already compatible, copying ${inputPath} -> ${outputPath}`)
  await fsp.copyFile(inputPath, outputPath)
  const stats = await fsp.stat(outputPath)
  console.log(`[copy] done ${outputPath} (${stats.size} bytes)`)
  return stats.size
}

async function runTranscode(inputPath, outputPath) {
  console.log(`[ffmpeg] start ${inputPath} -> ${outputPath}`)

  const scaleFilter = `scale='min(${MAX_OUTPUT_WIDTH},iw)':'min(${MAX_OUTPUT_HEIGHT},ih)':force_original_aspect_ratio=decrease`

  await runProcess("ffmpeg", [
    "-y",
    "-i", inputPath,
    "-vf", scaleFilter,
    "-c:v", "libx264",
    "-preset", FFMPEG_PRESET,
    "-crf", FFMPEG_CRF,
    "-pix_fmt", "yuv420p",
    "-movflags", "+faststart",
    "-threads", "1",
    "-c:a", "aac",
    "-b:a", "128k",
    outputPath
  ])

  const stats = await fsp.stat(outputPath)
  console.log(`[ffmpeg] done ${outputPath} (${stats.size} bytes)`)

  return stats.size
}

async function claimJob() {
  const { data, error } = await supabase.rpc("claim_video_job", {
    worker_name: WORKER_ID
  })

  if (error) {
    throw new Error(`claim_video_job failed: ${error.message}`)
  }

  if (!data) return null
  return Array.isArray(data) ? data[0] ?? null : data
}

async function failJob(id, message) {
  const safeMessage = String(message || "unknown error").slice(0, 5000)

  const { error } = await supabase.rpc("fail_video_job", {
    job_id: id,
    error: safeMessage
  })

  if (error) {
    console.error(`[job ${id}] fail_video_job error: ${error.message}`)
  }
}

async function completeJob(id, outputPath, size) {
  const { error } = await supabase.rpc("complete_video_job", {
    job_id: id,
    output_path: outputPath,
    output_size: size
  })

  if (error) {
    throw new Error(`complete_video_job failed: ${error.message}`)
  }
}

async function updateMetadata(jobId, metadata, inputSize) {
  const { error } = await supabase
    .from("video_jobs")
    .update({
      video_duration_seconds: metadata.durationSeconds,
      video_width: metadata.width,
      video_height: metadata.height,
      input_size_bytes: inputSize
    })
    .eq("id", jobId)

  if (error) {
    console.error(`[job ${jobId}] metadata update failed: ${error.message}`)
  }
}

async function requeueStaleJobs() {
  console.log(`[requeue] checking stale jobs older than ${STALE_REQUEUE_MINUTES} minutes`)

  const { data, error } = await supabase.rpc("requeue_stale_video_jobs", {
    stale_minutes: Number(STALE_REQUEUE_MINUTES)
  })

  if (error) {
    console.error(`[requeue] error: ${error.message}`)
    return
  }

  console.log(`[requeue] done, jobs moved back to pending: ${data ?? 0}`)
}

function buildOutputStoragePath(job) {
  const videoId = safeString(job.video_id) || "unknown-video"
  return `processed/${videoId}/${job.id}.mp4`
}

async function processJob(job) {
  const jobId = job.id
  const originalPath = job.original_path
  const attempts = Number(job.attempts || 0)

  console.log(`[job ${jobId}] claimed`)
  console.log(`[job ${jobId}] original_path=${originalPath}`)
  console.log(`[job ${jobId}] bucket=${SUPABASE_STORAGE_BUCKET}`)
  console.log(`[job ${jobId}] attempts=${attempts}`)
  console.log(`[job ${jobId}] video_id=${job.video_id}`)

  if (attempts > Number(MAX_JOB_ATTEMPTS)) {
    throw new Error(`max attempts exceeded (${attempts})`)
  }

  const tempDir = path.join(TEMP_DIR, jobId)
  await ensureDir(tempDir)

  const inputPath = path.join(tempDir, "input")
  const outputPath = path.join(tempDir, "output.mp4")
  const outputStoragePath = buildOutputStoragePath(job)

  try {
    const inputSize = await downloadFromStorageToFile(originalPath, inputPath)

    if (inputSize > Number(MAX_INPUT_SIZE_BYTES)) {
      throw new Error(
        `input file too large: ${inputSize} bytes (limit ${MAX_INPUT_SIZE_BYTES})`
      )
    }

    const metadata = await ffprobeFile(inputPath)
    await updateMetadata(jobId, metadata, inputSize)

    let outputSize = 0

    if (isAlreadyCompatible(metadata)) {
      outputSize = await copyCompatibleVideo(inputPath, outputPath)
    } else {
      outputSize = await runTranscode(inputPath, outputPath)
    }

    await uploadFileToStorage(outputPath, outputStoragePath)
    await completeJob(jobId, outputStoragePath, outputSize)

    console.log(`[job ${jobId}] completed`)
  } catch (err) {
    console.error(`[job ${jobId}] failed`, err)
    await failJob(jobId, err.stack || err.message || String(err))
  } finally {
    await fsp.rm(tempDir, { recursive: true, force: true })
  }
}

async function workerLoop() {
  await ensureDir(TEMP_DIR)

  console.log(`[worker] started`)
  console.log(`[worker] id=${WORKER_ID}`)
  console.log(`[worker] bucket=${SUPABASE_STORAGE_BUCKET}`)
  console.log(`[worker] poll_interval_ms=${POLL_INTERVAL_MS}`)
  console.log(`[worker] max_input_size_bytes=${MAX_INPUT_SIZE_BYTES}`)
  console.log(`[worker] max_output=${MAX_OUTPUT_WIDTH}x${MAX_OUTPUT_HEIGHT}`)
  console.log(`[worker] stale_requeue_minutes=${STALE_REQUEUE_MINUTES}`)

  let loopCount = 0

  while (true) {
    try {
      loopCount += 1

      if (loopCount % Number(REQUEUE_CHECK_EVERY_LOOPS) === 0) {
        await requeueStaleJobs()
      }

      const job = await claimJob()

      if (!job) {
        await sleep(Number(POLL_INTERVAL_MS))
        continue
      }

      await processJob(job)
    } catch (err) {
      console.error("[worker] loop error", err)
      await sleep(Number(POLL_INTERVAL_MS))
    }
  }
}

workerLoop().catch((err) => {
  console.error("[fatal] worker crashed", err)
  process.exit(1)
})
