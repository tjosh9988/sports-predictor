import 'package:flutter/material.dart';

class SportIcon extends StatelessWidget {
  final String sportSlug;
  final double size;
  final Color? color;

  const SportIcon({
    super.key,
    required this.sportSlug,
    this.size = 24,
    this.color,
  });

  @override
  Widget build(BuildContext context) {
    IconData iconData;
    switch (sportSlug.toLowerCase()) {
      case 'football':
      case 'soccer':
        iconData = Icons.sports_soccer;
        break;
      case 'basketball':
        iconData = Icons.sports_basketball;
        break;
      case 'tennis':
        iconData = Icons.sports_tennis;
        break;
      case 'nfl':
      case 'american_football':
        iconData = Icons.sports_football;
        break;
      case 'cricket':
        iconData = Icons.sports_cricket;
        break;
      case 'nhl':
      case 'hockey':
        iconData = Icons.sports_hockey;
        break;
      case 'mlb':
      case 'baseball':
        iconData = Icons.sports_baseball;
        break;
      default:
        iconData = Icons.sports_score;
    }

    return Icon(iconData, size: size, color: color);
  }
}
