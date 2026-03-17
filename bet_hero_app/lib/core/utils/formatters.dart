import 'package:intl/intl.dart';

class AppFormatters {
  static String formatOdds(double odds) => odds.toStringAsFixed(2);
  
  static String formatDate(DateTime date) => DateFormat('dd MMM yyyy, HH:mm').format(date);
  
  static String formatCurrency(double amount) => NumberFormat.currency(symbol: '\$').format(amount);
  
  static String formatPercentage(double value) => '${value.toStringAsFixed(1)}%';
}
