import React, { useState, useEffect, useCallback } from 'react';
import { listComments, createComment, deleteComment } from '../../api/client';
import CommentInput from './CommentInput';

interface Comment {
  id: string;
  text: string;
  author: string;
  targetType: string;
  targetId: string;
  parentId: string | null;
  createdAt: string;
}

interface CommentThreadProps {
  targetType: string;
  targetId: string;
}

export default function CommentThread({ targetType, targetId }: CommentThreadProps) {
  const [comments, setComments] = useState<Comment[]>([]);
  const [replyTo, setReplyTo] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchComments = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await listComments(targetType, targetId);
      setComments((data.comments as Comment[]) || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load comments');
    } finally {
      setLoading(false);
    }
  }, [targetType, targetId]);

  useEffect(() => {
    fetchComments();
  }, [fetchComments]);

  const handleSubmit = async (text: string, parentId?: string) => {
    setError(null);
    try {
      await createComment({
        text,
        targetType,
        targetId,
        parentId: parentId || null,
      });
      setReplyTo(null);
      await fetchComments();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to post comment');
    }
  };

  const handleDelete = async (id: string) => {
    setError(null);
    try {
      await deleteComment(id);
      await fetchComments();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete comment');
    }
  };

  const topLevel = comments.filter((c) => !c.parentId);
  const replies = (parentId: string) => comments.filter((c) => c.parentId === parentId);

  const formatTime = (iso: string) => {
    try {
      const d = new Date(iso);
      const now = new Date();
      const diff = now.getTime() - d.getTime();
      const mins = Math.floor(diff / 60000);
      if (mins < 1) return 'just now';
      if (mins < 60) return `${mins}m ago`;
      const hrs = Math.floor(mins / 60);
      if (hrs < 24) return `${hrs}h ago`;
      return d.toLocaleDateString();
    } catch {
      return iso;
    }
  };

  const renderComment = (comment: Comment, depth = 0) => (
    <div
      key={comment.id}
      className={`${depth > 0 ? 'ml-6 border-l-2 border-border pl-3' : ''}`}
    >
      <div className="py-2 group">
        <div className="flex items-center gap-2">
          <div className="w-6 h-6 rounded-full bg-primary/20 flex items-center justify-center text-xs text-primary font-medium">
            {comment.author.charAt(0).toUpperCase()}
          </div>
          <span className="text-xs font-medium text-gray-300">{comment.author}</span>
          <span className="text-xs text-gray-600">{formatTime(comment.createdAt)}</span>
          <div className="flex-1" />
          <div className="opacity-0 group-hover:opacity-100 flex gap-1 transition">
            <button
              onClick={() => setReplyTo(comment.id)}
              className="text-xs text-gray-500 hover:text-gray-300 px-1"
              aria-label={`Reply to ${comment.author}'s comment`}
            >
              Reply
            </button>
            <button
              onClick={() => handleDelete(comment.id)}
              className="text-xs text-gray-500 hover:text-red-400 px-1"
              aria-label={`Delete comment by ${comment.author}`}
            >
              Delete
            </button>
          </div>
        </div>
        <p className="text-sm text-gray-300 mt-1 ml-8">{comment.text}</p>
      </div>

      {/* Reply input */}
      {replyTo === comment.id && (
        <div className="ml-8 mt-1 mb-2">
          <CommentInput
            onSubmit={(text) => handleSubmit(text, comment.id)}
            onCancel={() => setReplyTo(null)}
            placeholder="Write a reply..."
            compact
          />
        </div>
      )}

      {/* Nested replies */}
      {replies(comment.id).map((reply) => renderComment(reply, depth + 1))}
    </div>
  );

  return (
    <div className="rounded-lg border border-border bg-gray-900/50">
      <div className="px-4 py-2.5 border-b border-border bg-gray-900/80">
        <h4 className="text-sm font-semibold text-gray-200">
          Comments ({comments.length})
        </h4>
      </div>

      {error && (
        <div className="mx-3 mt-3 px-3 py-2 rounded-lg bg-red-500/10 border border-red-500/30 text-xs text-red-400">
          {error}
        </div>
      )}

      <div className="p-3 space-y-1 max-h-80 overflow-y-auto">
        {loading && <p className="text-xs text-gray-500">Loading...</p>}
        {!loading && topLevel.length === 0 && (
          <p className="text-xs text-gray-500 py-4 text-center">No comments yet</p>
        )}
        {topLevel.map((c) => renderComment(c))}
      </div>

      <div className="px-3 pb-3 border-t border-border pt-3">
        <CommentInput onSubmit={(text) => handleSubmit(text)} placeholder="Add a comment..." />
      </div>
    </div>
  );
}
