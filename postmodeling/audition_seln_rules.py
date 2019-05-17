from triage.component.audition.rules_maker import SimpleRuleMaker, RandomGroupRuleMaker, TwoMetricsRuleMaker, create_selection_grid

Rule1 = SimpleRuleMaker()
Rule1.add_rule_best_current_value(metric='precision@', parameter='50_abs', n=3)
Rule1.add_rule_best_average_value(metric='precision@', parameter='50_abs', n=3)
Rule1.add_rule_lowest_metric_variance(metric='precision@', parameter='50_abs', n=3)
Rule1.add_rule_most_frequent_best_dist(
	metric='precision@',
	parameter='50_abs',
	dist_from_best_case=[0.05],
	n=3
)
Rule1.add_rule_best_avg_recency_weight(
	metric='precision@',
	parameter='50_abs',
	curr_weight=[1.5, 2.0, 5.0],
	decay_type=['linear'],
	n=1
)
Rule1.add_rule_best_avg_var_penalized(
	metric='precision@',
	parameter='50_abs',
	stdev_penalty=0.5,
	n=1
)
Rule2 = RandomGroupRuleMaker(n=1)

Rule3 = TwoMetricsRuleMaker()
Rule3.add_rule_best_average_two_metrics(
	metric1='precision@',
	parameter1='50_abs',
	metric2='precision@',
	parameter2='100_abs',
	metric1_weight=[0.5],
	n=1
)
seln_rules = create_selection_grid(Rule1, Rule2, Rule3)