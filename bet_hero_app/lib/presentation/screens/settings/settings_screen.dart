import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import '../../../core/config.dart';
import '../../../core/di.dart';
import '../../../core/theme.dart';
import '../../widgets/common/app_bar_widget.dart';
import '../../widgets/common/confirm_dialog.dart';
import '../../widgets/common/section_header.dart';


class SettingsScreen extends ConsumerWidget {
  const SettingsScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final user = ref.watch(authServiceProvider).currentUser;

    return Scaffold(
      appBar: const AppBarWidget(title: 'SETTINGS', showLogo: false),
      body: ListView(
        children: [
          const SectionHeader(title: 'Account'),
          _buildUserCard(user),
          _settingsTile(Icons.lock_outline, 'Change Password', () {}),
          _settingsTile(Icons.logout, 'Sign Out', () => _showSignOutDialog(context, ref), isDestructive: true),
          
          const SectionHeader(title: 'Preferences'),
          _settingsTile(Icons.sports_soccer, 'Favorite Sports', () {}),
          _settingsTile(Icons.format_list_numbered, 'Odds Format', () {}),
          _settingsTile(Icons.attach_money, 'Default Stake', () {}, trailing: '£10.00'),
          
          const SectionHeader(title: 'Notifications'),
          _switchTile('New Accumulators', true, (val) {}),
          _switchTile('Accumulator Results', true, (val) {}),
          _switchTile('Match Reminders', false, (val) {}),
          _settingsTile(Icons.timer_outlined, 'Quiet Hours', () {}),
          
          const SectionHeader(title: 'About'),
          _settingsTile(Icons.description_outlined, 'Terms of Service', () {}),
          _settingsTile(Icons.privacy_tip_outlined, 'Privacy Policy', () {}),
          _settingsTile(Icons.health_and_safety_outlined, 'Responsible Gambling', () {}),
          _settingsTile(Icons.star_outline, 'Rate the App', () {}),
          
          Padding(
            padding: const EdgeInsets.symmetric(vertical: 32),
            child: Center(
              child: Text(
                'Beta Version ${AppConfig.appVersion} (${AppConfig.buildNumber})',
                style: const TextStyle(color: AppTheme.textSecondary, fontSize: 12),
              ),
            ),
          ),
          const SizedBox(height: 50),
        ],
      ),
    );
  }

  Widget _buildUserCard(dynamic user) {
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: AppTheme.cardBackground,
        borderRadius: BorderRadius.circular(12),
      ),
      child: Row(
        children: [
          CircleAvatar(
            backgroundColor: AppTheme.primaryGold.withOpacity(0.1),
            child: const Icon(Icons.person, color: AppTheme.primaryGold),
          ),
          const SizedBox(width: 16),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(user?.email?.split('@')[0].toUpperCase() ?? 'GUEST USER', style: const TextStyle(fontWeight: FontWeight.bold)),
                Text(user?.email ?? 'Not logged in', style: const TextStyle(color: AppTheme.textSecondary, fontSize: 12)),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _settingsTile(IconData icon, String title, VoidCallback onTap, {String? trailing, bool isDestructive = false}) {
    return ListTile(
      leading: Icon(icon, color: isDestructive ? AppTheme.dangerRed : AppTheme.textSecondary, size: 20),
      title: Text(title, style: TextStyle(color: isDestructive ? AppTheme.dangerRed : AppTheme.textPrimary, fontSize: 14)),
      trailing: trailing != null 
          ? Text(trailing, style: const TextStyle(color: AppTheme.primaryGold, fontSize: 14, fontWeight: FontWeight.bold))
          : const Icon(Icons.chevron_right, size: 16, color: AppTheme.textSecondary),
      onTap: onTap,
    );
  }

  Widget _switchTile(String title, bool value, ValueChanged<bool> onChanged) {
    return SwitchListTile(
      value: value,
      onChanged: onChanged,
      title: Text(title, style: const TextStyle(color: AppTheme.textPrimary, fontSize: 14)),
      activeColor: AppTheme.primaryGold,
      activeTrackColor: AppTheme.primaryGold.withOpacity(0.3),
    );
  }

  void _showSignOutDialog(BuildContext context, WidgetRef ref) async {
    final confirm = await showDialog<bool>(
      context: context,
      builder: (context) => const ConfirmDialog(
        title: 'Sign Out',
        message: 'Are you sure you want to log out of your account?',
        confirmLabel: 'SIGN OUT',
        isDestructive: true,
      ),
    );
    
    if (confirm == true) {
      await ref.read(authServiceProvider).signOut();
      if (context.mounted) context.go('/login');
    }
  }
}
