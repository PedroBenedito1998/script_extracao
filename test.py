import os
import re
import pandas as pd
from pathlib import Path
from datetime import datetime

class NetworkSimulationProcessor:
    def __init__(self, simulation_path):
        self.sim_dir = Path(simulation_path)
        if not self.sim_dir.exists():
            raise FileNotFoundError(f"Directory not found: {simulation_path}")
        
        self.sim_id = self.sim_dir.name
        self.results = []
        self.valid_qdisc_types = ['pie', 'codel', 'dualpi2', 'fq_codel']
        
    @staticmethod
    def _convert_units(value, unit):
        """Convert storage units to bytes"""
        unit = unit.lower()
        conversions = {
            'kib': 1024,
            'mib': 1024**2,
            'gib': 1024**3,
            'tib': 1024**4
        }
        return value * conversions.get(unit, 1)

    @staticmethod
    def _convert_time(time_str):
        """Convert time units to nanoseconds"""
        if not time_str:
            return 0
        if 'us' in time_str:
            return float(time_str.replace('us', '')) * 1000
        elif 'ms' in time_str:
            return float(time_str.replace('ms', '')) * 1e6
        return float(time_str)

    def _extract_metric(self, content, pattern, convert_func=None):
        """Generic metric extractor with optional conversion"""
        match = re.search(pattern, content)
        if not match:
            return None
        value = match.group(1)
        return convert_func(value) if convert_func else value

    def _parse_common_metrics(self, content):
        """Extract metrics common to all qdisc types"""
        common_metrics = {
            'sent_bytes': (r'Sent (\d+) bytes', int),
            'sent_pkts': (r'Sent \d+ bytes (\d+) pkt', int),
            'dropped': (r'dropped (\d+)', int),
            'overlimits': (r'overlimits (\d+)', int),
            'requeues': (r'requeues (\d+)', int),
            'backlog_bytes': (r'backlog (\d+)b', int),
            'backlog_pkts': (r'backlog \d+b (\d+)p', int)
        }
        
        return {
            field: self._extract_metric(content, pattern, conv)
            for field, (pattern, conv) in common_metrics.items()
        }

    def _parse_pie_metrics(self, content):
        """Extract PIE-specific metrics"""
        pie_metrics = {
            'prob': (r'prob (\d+)', float),
            'delay': (r'delay ([\d.]+(?:us|ms))', self._convert_time),
            'pkts_in': (r'pkts_in (\d+)', int),
            'pkts_overlimit': (r'overlimit (\d+)', int),
            'maxq': (r'maxq (\d+)', int),
            'ecn_mark': (r'ecn_mark (\d+)', int),
            'target': (r'target ([\d.]+ms)', self._convert_time),
            'tupdate': (r'tupdate ([\d.]+ms)', self._convert_time),
            'alpha': (r'alpha (\d+)', int),
            'beta': (r'beta (\d+)', int)
        }
        
        return {
            field: self._extract_metric(content, pattern, conv)
            for field, (pattern, conv) in pie_metrics.items()
        }

    def _parse_codel_metrics(self, content):
        """Extract CoDel-specific metrics"""
        codel_metrics = {
            'count': (r'count (\d+)', int),
            'lastcount': (r'lastcount (\d+)', int),
            'ldelay': (r'ldelay ([\d.]+us)', self._convert_time),
            'drop_next': (r'drop_next ([\d.]+us)', self._convert_time),
            'maxpacket': (r'maxpacket (\d+)', int),
            'ecn_mark': (r'ecn_mark (\d+)', int),
            'drop_overlimit': (r'drop_overlimit (\d+)', int),
            'target': (r'target ([\d.]+ms)', self._convert_time),
            'interval': (r'interval ([\d.]+ms)', self._convert_time)
        }
        
        return {
            field: self._extract_metric(content, pattern, conv)
            for field, (pattern, conv) in codel_metrics.items()
        }

    def _parse_dualpi2_metrics(self, content):
        """Extract DualPI2-specific metrics"""
        dualpi2_metrics = {
            'prob': (r'prob ([\d.]+)', float),
            'delay_c': (r'delay_c ([\d.]+us)', self._convert_time),
            'delay_l': (r'delay_l ([\d.]+us)', self._convert_time),
            'pkts_in_c': (r'pkts_in_c (\d+)', int),
            'pkts_in_l': (r'pkts_in_l (\d+)', int),
            'maxq': (r'maxq (\d+)', int),
            'ecn_mark': (r'ecn_mark (\d+)', int),
            'step_marks': (r'step_marks (\d+)', int),
            'credit': (r'credit (-?\d+)', int),
            'target': (r'target ([\d.]+ms)', self._convert_time),
            'tupdate': (r'tupdate ([\d.]+ms)', self._convert_time),
            'alpha': (r'alpha ([\d.]+)', float),
            'beta': (r'beta ([\d.]+)', float),
            'coupling_factor': (r'coupling_factor (\d+)', int)
        }
        
        return {
            field: self._extract_metric(content, pattern, conv)
            for field, (pattern, conv) in dualpi2_metrics.items()
        }

    def _parse_fq_codel_metrics(self, content):
        """Extract FQ_CoDel-specific metrics"""
        fq_codel_metrics = {
            'maxpacket': (r'maxpacket (\d+)', int),
            'drop_overlimit': (r'drop_overlimit (\d+)', int),
            'new_flow_count': (r'new_flow_count (\d+)', int),
            'ecn_mark': (r'ecn_mark (\d+)', int),
            'new_flows_len': (r'new_flows_len (\d+)', int),
            'old_flows_len': (r'old_flows_len (\d+)', int),
            'target': (r'target ([\d.]+ms)', self._convert_time),
            'interval': (r'interval ([\d.]+ms)', self._convert_time),
            'quantum': (r'quantum (\d+)', int),
            'memory_limit': (r'memory_limit (\d+\w+)', lambda x: self._convert_units(float(x[:-1]), x[-1:])),
            'drop_batch': (r'drop_batch (\d+)', int)
        }
        
        return {
            field: self._extract_metric(content, pattern, conv)
            for field, (pattern, conv) in fq_codel_metrics.items()
        }

    def _parse_qdisc_content(self, content):
        """Parse qdisc content and return metrics"""
        qdisc_match = re.match(r'qdisc (\w+)', content)
        if not qdisc_match:
            return None
            
        qdisc_type = qdisc_match.group(1).lower()
        if qdisc_type not in self.valid_qdisc_types:
            return None

        metrics = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'qdisc_type': qdisc_type,
            **self._parse_common_metrics(content)
        }

        # Add type-specific metrics
        if qdisc_type == 'pie':
            metrics.update(self._parse_pie_metrics(content))
        elif qdisc_type == 'codel':
            metrics.update(self._parse_codel_metrics(content))
        elif qdisc_type == 'dualpi2':
            metrics.update(self._parse_dualpi2_metrics(content))
        elif qdisc_type == 'fq_codel':
            metrics.update(self._parse_fq_codel_metrics(content))

        return metrics

    def _parse_throughput_log(self, content):
        """Parse throughput log content"""
        pattern = (
            r'\*\*\* Download Progress Summary as of (.*?) \*\*\*.*?'
            r'\[#\w+ (\d+\.?\d*)([KMGT]?iB)/(\d+\.?\d*)([KMGT]?iB).*?'
            r'DL:(\d+\.?\d*)([KMGT]?iB)'
        )
        matches = re.findall(pattern, content, re.DOTALL)
        
        throughput_data = []
        for match in matches:
            throughput_data.append({
                'timestamp': datetime.strptime(match[0], '%a %b %d %H:%M:%S %Y'),
                'downloaded_bytes': self._convert_units(float(match[1]), match[2]),
                'total_size_bytes': self._convert_units(float(match[3]), match[4]),
                'throughput_bps': self._convert_units(float(match[5]), match[6]) * 8,
                'progress_pct': (float(match[1]) / float(match[3])) * 100 if float(match[3]) > 0 else 0
            })
        return throughput_data

    def _process_qdisc_files(self):
        """Process all qdisc files for the simulation"""
        interfaces = ['eth1', 'eth7']
        roles = ['client', 'server']
        
        for role in roles:
            for interface in interfaces:
                file_path = self.sim_dir / role / f"{self.sim_id}_Router_Queue_Size_{role}_{interface}.txt"
                if not file_path.exists():
                    print(f"Warning: File not found - {file_path}")
                    continue
                
                with open(file_path, 'r') as f:
                    content = f.read()
                    for block in content.split('qdisc')[1:]:
                        metrics = self._parse_qdisc_content('qdisc' + block)
                        if metrics:
                            metrics.update({
                                'test_id': self.sim_id,
                                'data_source': 'qdisc',
                                'origin': role,
                                'interface': interface,
                                'metric_type': 'queue_metrics'
                            })
                            self.results.append(metrics)

    def _process_throughput_logs(self):
        """Process throughput logs for the simulation"""
        log_sources = {
            'client_dos': self.sim_dir / 'output_client_2',
            'client_juan': self.sim_dir / 'output_client_juan'
        }
        
        for origin, log_dir in log_sources.items():
            if not log_dir.exists():
                print(f"Warning: Log directory not found - {log_dir}")
                continue
                
            for log_file in log_dir.glob(f"F-Stack_Client-{origin.split('_')[-1].capitalize()}_*_{self.sim_id}.txt"):
                with open(log_file, 'r') as f:
                    content = f.read()
                    for metrics in self._parse_throughput_log(content):
                        metrics.update({
                            'test_id': self.sim_id,
                            'data_source': 'fstack',
                            'origin': origin,
                            'interface': 'N/A',
                            'metric_type': 'throughput',
                            'qdisc_type': 'N/A'
                        })
                        self.results.append(metrics)

    def process(self):
        """Main processing method"""
        print(f"\nStarting processing for simulation: {self.sim_id}")
        
        self._process_qdisc_files()
        self._process_throughput_logs()
        
        if not self.results:
            print("Warning: No data was processed")
            return False
            
        return True

    def save_results(self, output_dir=None):
        """Save processed results to CSV"""
        if not self.results:
            print("Error: No results to save")
            return None
            
        df = pd.DataFrame(self.results)
        
        # Ensure consistent column order
        base_columns = [
            'test_id', 'timestamp', 'data_source', 'origin', 
            'interface', 'metric_type', 'qdisc_type'
        ]
        other_columns = [col for col in df.columns if col not in base_columns]
        df = df[base_columns + other_columns]
        
        output_path = Path(output_dir or self.sim_dir) / f"{self.sim_id}_metrics.csv"
        df.to_csv(output_path, index=False)
        
        print(f"\nResults saved to: {output_path}")
        return output_path

def main():
    print("=== Network Simulation Data Processor ===")
    
    while True:
        sim_path = input("\nEnter full simulation path (or 'quit' to exit): ").strip()
        if sim_path.lower() in ('quit', 'exit'):
            break
            
        try:
            processor = NetworkSimulationProcessor(sim_path)
            if processor.process():
                output_file = processor.save_results()
                print(f"\nSuccessfully processed simulation {processor.sim_id}")
                print(f"Output file: {output_file}")
        except Exception as e:
            print(f"\nError processing simulation: {str(e)}")
            print("Please check:")
            print(f"1. The path exists: {sim_path}")
            print("2. The directory contains the expected files")
            print("3. You have proper read permissions")

if __name__ == "__main__":
    main()