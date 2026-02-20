/**
 * ui/file-upload.js — File upload and drag/drop handling
 *
 * Extracted from index.html lines 570-590.
 * Updated to support multi-format binary file uploads (gz, zip, nar, jar, docx, xlsx, tar.gz).
 *
 * FIX MED: Added null checks for DOM elements and file input validation.
 */

/** Uploaded file content (raw text) */
let uploadedContent = '';

/** Uploaded file name */
let uploadedName = '';

/** Uploaded raw bytes for binary formats (Uint8Array or null) */
let uploadedBytes = null;

/** File extensions that should be read as ArrayBuffer (binary) */
const BINARY_EXTENSIONS = [
  '.gz', '.zip', '.nar', '.jar', '.tgz', '.docx', '.xlsx'
];

/**
 * Check if a filename should be read as binary.
 * @param {string} name
 * @returns {boolean}
 */
function isBinaryFile(name) {
  const lower = name.toLowerCase();
  // Check for .tar.gz specifically (compound extension)
  if (lower.endsWith('.tar.gz')) return true;
  // Check for .xml.gz, .json.gz (compound extensions ending in .gz)
  if (lower.endsWith('.xml.gz') || lower.endsWith('.json.gz')) return true;
  return BINARY_EXTENSIONS.some(ext => lower.endsWith(ext));
}

/**
 * Get the current uploaded content.
 * @returns {string}
 */
export function getUploadedContent() {
  return uploadedContent;
}

/**
 * Set the uploaded content programmatically (used by sample flow loaders).
 * @param {string} content
 * @param {string} name
 */
export function setUploadedContent(content, name) {
  uploadedContent = content;
  uploadedName = name;
  uploadedBytes = null;
}

/**
 * Get the current uploaded file name.
 * @returns {string}
 */
export function getUploadedName() {
  return uploadedName;
}

/**
 * Get the uploaded raw bytes (for binary file formats).
 * @returns {Uint8Array|null}
 */
export function getUploadedBytes() {
  return uploadedBytes;
}

/**
 * Handle the file input change event — read the selected file.
 * Detects file type by extension and reads as text or ArrayBuffer accordingly.
 */
export function handleFile() {
  const fileInput = document.getElementById('fileInput');
  if (!fileInput) return Promise.resolve();
  const f = fileInput.files[0];
  if (!f) return Promise.resolve();

  uploadedName = f.name;
  const fileNameEl = document.getElementById('fileName');
  if (fileNameEl) {
    fileNameEl.textContent = 'Loaded: ' + f.name;
    fileNameEl.classList.remove('hidden');
  }

  const binary = isBinaryFile(f.name);

  return new Promise(resolve => {
    const reader = new FileReader();
    reader.onload = e => {
      if (binary) {
        uploadedBytes = new Uint8Array(e.target.result);
        uploadedContent = ''; // no text content for binary files
      } else {
        uploadedContent = e.target.result;
        uploadedBytes = null;
      }
      resolve();
    };
    reader.onerror = () => resolve();

    if (binary) {
      reader.readAsArrayBuffer(f);
    } else {
      reader.readAsText(f);
    }
  });
}

/**
 * Initialize file upload listeners (drag/drop zone + file input change).
 * Extracted from index.html lines 571-579.
 *
 * FIX MED: Guards against missing DOM elements.
 */
export function initFileUpload() {
  const fileInput = document.getElementById('fileInput');
  const dropZone = document.getElementById('fileDropZone');

  if (!fileInput || !dropZone) return;

  // Accept all supported formats
  fileInput.setAttribute('accept',
    '.xml,.json,.sql,.txt,.csv,.gz,.zip,.nar,.jar,.tgz,.tar.gz,.docx,.xlsx'
  );

  dropZone.addEventListener('click', () => fileInput.click());

  dropZone.addEventListener('dragover', e => {
    e.preventDefault();
    dropZone.style.borderColor = 'var(--primary)';
  });

  dropZone.addEventListener('dragleave', () => {
    dropZone.style.borderColor = 'var(--border)';
  });

  dropZone.addEventListener('drop', async (e) => {
    e.preventDefault();
    dropZone.style.borderColor = 'var(--border)';
    const file = e.dataTransfer.files[0];
    if (file) {
      // Read the dropped file directly instead of setting fileInput.files
      uploadedName = file.name;
      const fileNameEl = document.getElementById('fileName');
      if (fileNameEl) {
        fileNameEl.textContent = 'Loaded: ' + file.name;
        fileNameEl.classList.remove('hidden');
      }
      const binary = isBinaryFile(file.name);
      await new Promise(resolve => {
        const reader = new FileReader();
        reader.onload = ev => {
          if (binary) {
            uploadedBytes = new Uint8Array(ev.target.result);
            uploadedContent = '';
          } else {
            uploadedContent = ev.target.result;
            uploadedBytes = null;
          }
          resolve();
        };
        reader.onerror = () => resolve();
        if (binary) {
          reader.readAsArrayBuffer(file);
        } else {
          reader.readAsText(file);
        }
      });
      // Dispatch change event so main.js handler triggers parseInput
      fileInput.dispatchEvent(new Event('change', { bubbles: true }));
    }
  });

  // Note: change event is handled by main.js which calls handleFile() + parseInput()
}
