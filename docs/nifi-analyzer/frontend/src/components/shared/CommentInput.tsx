import React, { useState, useRef, useEffect } from 'react';

interface CommentInputProps {
  onSubmit: (text: string) => void;
  onCancel?: () => void;
  placeholder?: string;
  compact?: boolean;
}

export default function CommentInput({
  onSubmit,
  onCancel,
  placeholder = 'Add a comment...',
  compact = false,
}: CommentInputProps) {
  const [text, setText] = useState('');
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (compact && inputRef.current) {
      inputRef.current.focus();
    }
  }, [compact]);

  const handleSubmit = () => {
    const trimmed = text.trim();
    if (!trimmed) return;
    onSubmit(trimmed);
    setText('');
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      handleSubmit();
    }
    if (e.key === 'Escape' && onCancel) {
      onCancel();
    }
  };

  return (
    <div className="flex flex-col gap-2">
      <textarea
        ref={inputRef}
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        rows={compact ? 2 : 3}
        className="w-full px-3 py-2 text-sm rounded-lg bg-gray-800 border border-border text-gray-200 placeholder-gray-600 focus:outline-none focus:border-primary/50 resize-none"
        aria-label={placeholder}
      />
      <div className="flex items-center justify-between">
        <span className="text-xs text-gray-600">Ctrl+Enter to submit</span>
        <div className="flex gap-2">
          {onCancel && (
            <button
              onClick={onCancel}
              className="px-3 py-1 text-xs rounded bg-gray-800 text-gray-400 hover:text-gray-200 transition"
            >
              Cancel
            </button>
          )}
          <button
            onClick={handleSubmit}
            disabled={!text.trim()}
            className="px-3 py-1 text-xs rounded bg-primary/20 text-primary hover:bg-primary/30 transition disabled:opacity-40 disabled:cursor-not-allowed"
            aria-label="Submit comment"
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}
