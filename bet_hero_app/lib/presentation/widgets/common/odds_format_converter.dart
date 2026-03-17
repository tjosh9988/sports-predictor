class OddsFormatConverter {
  static String convert(double decimalOdds, String format) {
    switch (format.toLowerCase()) {
      case 'fractional':
        return _toFractional(decimalOdds);
      case 'american':
        return _toAmerican(decimalOdds);
      case 'decimal':
      default:
        return decimalOdds.toStringAsFixed(2);
    }
  }

  static String _toFractional(double decimal) {
    if (decimal <= 1.0) return "0/1";
    
    double val = decimal - 1;
    // Simple rounding to common fractions for betting context
    // In a real app, use a more robust GCD based fraction reducer
    if ((val * 10).round() % 10 == 0) return "${val.toInt()}/1";
    if ((val * 2).round() % 2 == 0) return "${(val * 2).toInt()}/2";
    
    return "${(val * 100).toInt()}/100";
  }

  static String _toAmerican(double decimal) {
    if (decimal >= 2.0) {
      return "+${((decimal - 1) * 100).round()}";
    } else {
      return "-${(100 / (decimal - 1)).round()}";
    }
  }
}
