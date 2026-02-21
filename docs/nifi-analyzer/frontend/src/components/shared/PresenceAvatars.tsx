import React from 'react';
import type { PresenceUser } from '../../hooks/usePresence';

interface PresenceAvatarsProps {
  users: PresenceUser[];
  connected: boolean;
  maxDisplay?: number;
}

export default function PresenceAvatars({ users, connected, maxDisplay = 5 }: PresenceAvatarsProps) {
  if (users.length === 0) return null;

  const displayed = users.slice(0, maxDisplay);
  const overflow = users.length - maxDisplay;

  return (
    <div className="flex items-center gap-1.5" aria-label={`${users.length} users online`}>
      {/* Connection indicator */}
      <span
        className={`w-2 h-2 rounded-full ${connected ? 'bg-green-400' : 'bg-gray-600'}`}
        title={connected ? 'Connected' : 'Disconnected'}
      />

      {/* Avatar stack */}
      <div className="flex -space-x-2">
        {displayed.map((user) => (
          <div
            key={user.id}
            className="w-7 h-7 rounded-full border-2 border-gray-900 flex items-center justify-center text-xs font-medium text-white"
            style={{ backgroundColor: user.color }}
            title={`${user.user} (Step ${user.step + 1})`}
            aria-label={`${user.user} is on step ${user.step + 1}`}
          >
            {user.user.charAt(0).toUpperCase()}
          </div>
        ))}
        {overflow > 0 && (
          <div
            className="w-7 h-7 rounded-full border-2 border-gray-900 bg-gray-700 flex items-center justify-center text-xs font-medium text-gray-300"
            title={`${overflow} more users`}
          >
            +{overflow}
          </div>
        )}
      </div>
    </div>
  );
}
