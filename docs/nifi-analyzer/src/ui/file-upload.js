/**
 * ui/file-upload.js — File upload and drag/drop handling
 *
 * Extracted from index.html lines 570-590.
 *
 * FIX MED: Added null checks for DOM elements and file input validation.
 */

/** Uploaded file content (raw text) */
let uploadedContent = '';

/** Uploaded file name */
let uploadedName = '';

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
}

/**
 * Get the current uploaded file name.
 * @returns {string}
 */
export function getUploadedName() {
  return uploadedName;
}

/**
 * Handle the file input change event — read the selected file.
 * Extracted from index.html lines 582-590.
 */
export function handleFile() {
  const fileInput = document.getElementById('fileInput');
  if (!fileInput) return;
  const f = fileInput.files[0];
  if (!f) return;

  uploadedName = f.name;
  const fileNameEl = document.getElementById('fileName');
  if (fileNameEl) {
    fileNameEl.textContent = 'Loaded: ' + f.name;
    fileNameEl.classList.remove('hidden');
  }

  const reader = new FileReader();
  reader.onload = e => {
    uploadedContent = e.target.result;
  };
  reader.readAsText(f);
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

  dropZone.addEventListener('click', () => fileInput.click());

  dropZone.addEventListener('dragover', e => {
    e.preventDefault();
    dropZone.style.borderColor = 'var(--primary)';
  });

  dropZone.addEventListener('dragleave', () => {
    dropZone.style.borderColor = 'var(--border)';
  });

  dropZone.addEventListener('drop', e => {
    e.preventDefault();
    dropZone.style.borderColor = 'var(--border)';
    if (e.dataTransfer.files.length) {
      fileInput.files = e.dataTransfer.files;
      handleFile();
    }
  });

  fileInput.addEventListener('change', handleFile);
}
