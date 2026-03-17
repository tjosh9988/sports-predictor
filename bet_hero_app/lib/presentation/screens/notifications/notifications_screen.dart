import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../../../core/theme.dart';

import '../../widgets/common/app_bar_widget.dart';
import '../../widgets/common/empty_state_widget.dart';


class NotificationsScreen extends ConsumerWidget {
  const NotificationsScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    // Note: In a real app, this would be backed by a provider fetching from Supabase
    final List<Map<String, dynamic>> notifications = [
      {
        'title': '10-Odds Accumulator Ready',
        'body': 'Your daily AI prediction is now available for analysis.',
        'time': '10m ago',
        'isUnread': true,
        'type': 'new_prediction',
      },
      {
        'title': 'Prediction Won! 🎉',
        'body': 'Man City vs Arsenal finished 2-1 as predicted.',
        'time': '2h ago',
        'isUnread': false,
        'type': 'result',
      },
      {
        'title': 'Odds Alert',
        'body': 'Significant movement in Real Madrid vs Barcelona odds.',
        'time': 'Yesterday',
        'isUnread': false,
        'type': 'alert',
      },
    ];

    return Scaffold(
      appBar: const AppBarWidget(title: 'NOTIFICATIONS', showLogo: false),
      body: notifications.isEmpty
          ? const EmptyStateWidget(
              icon: Icons.notifications_none,
              title: 'No Notifications',
              message: 'Check back later for new predictions and match results.',
            )
          : Column(
              children: [
                _buildHeader(context),
                Expanded(
                  child: ListView.builder(
                    itemCount: notifications.length,
                    itemBuilder: (context, index) {
                      final n = notifications[index];
                      return _NotificationItem(notification: n);
                    },
                  ),
                ),
              ],
            ),
    );
  }

  Widget _buildHeader(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          const Text('Today', style: TextStyle(color: AppTheme.textSecondary, fontWeight: FontWeight.bold, fontSize: 12)),
          TextButton(
            onPressed: () {},
            child: const Text('Mark all as read', style: TextStyle(color: AppTheme.primaryGold, fontSize: 12)),
          ),
        ],
      ),
    );
  }
}

class _NotificationItem extends StatelessWidget {
  final Map<String, dynamic> notification;

  const _NotificationItem({required this.notification});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: notification['isUnread'] ? AppTheme.primaryGold.withOpacity(0.05) : Colors.transparent,
        border: Border(bottom: BorderSide(color: Colors.white.withOpacity(0.05))),
      ),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          _buildIcon(),
          const SizedBox(width: 16),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    Text(notification['title'], style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 14)),
                    Text(notification['time'], style: const TextStyle(color: AppTheme.textSecondary, fontSize: 10)),
                  ],
                ),
                const SizedBox(height: 4),
                Text(
                  notification['body'],
                  style: const TextStyle(color: AppTheme.textSecondary, fontSize: 12),
                ),
              ],
            ),
          ),
          if (notification['isUnread'])
            const Padding(
              padding: EdgeInsets.only(left: 8, top: 4),
              child: CircleAvatar(radius: 3, backgroundColor: AppTheme.primaryGold),
            ),
        ],
      ),
    );
  }

  Widget _buildIcon() {
    IconData icon;
    Color color;
    switch (notification['type']) {
      case 'new_prediction':
        icon = Icons.psychology;
        color = AppTheme.primaryGold;
        break;
      case 'result':
        icon = Icons.emoji_events;
        color = AppTheme.successGreen;
        break;
      default:
        icon = Icons.notifications_active;
        color = AppTheme.textSecondary;
    }
    return Container(
      padding: const EdgeInsets.all(8),
      decoration: BoxDecoration(color: color.withOpacity(0.1), shape: BoxShape.circle),
      child: Icon(icon, color: color, size: 20),
    );
  }
}
