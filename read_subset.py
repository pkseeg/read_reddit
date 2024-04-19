# adopted from https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/filter_file.py
import os
import random
import zstandard
import json
from tqdm import tqdm
from datetime import datetime


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)
			if not chunk:
				break
			lines = (buffer + chunk).split("\n")
			for line in lines[:-1]:
				yield line.strip(), file_handle.tell()
			buffer = lines[-1]
		reader.close()

def file_pairs(DIRS):
	pairs = []
	files1 = os.listdir(DIRS[0])
	files2 = os.listdir(DIRS[1])
	for f1 in files1:
		if not f1.lower().endswith('.zst'): continue
		month1 = f1[3:]
		for f2 in files2:
			if not f2.lower().endswith('.zst'): continue
			month2 = f2[3:]
			if month1 == month2:
				pairs.append((f1, f2))
				break
	return pairs
	
			


if __name__ == "__main__":
	directory = "/Volumes/reddit_drive/"
	DIRS = [directory + "reddit/comments/", directory + "reddit/submissions/"]

	subreddit_names = ['AskHistorians', 'AskAcademia', 'AskEngineers', 'AskCulinary', 'AskPhotography', 'AskMen', 'AskWomen', 'AskMenOver30', 'AskWomenOver30', 'AskOldPeople', 'AskEurope', 'AskUK', 'AskNYC', 'AskFrance', 'askSingapore', 'AskArgentina']
	output_files = ['ask_data/' + name + '.json' for name in subreddit_names]
	outs = {}
	for subreddit, output_file in zip(subreddit_names, output_files):
		outs[subreddit] = open(output_file, 'w')
		outs[subreddit].write("[")
		outs[subreddit+'_first'] = True

	pairs = file_pairs(DIRS)
	# randomly sample 5% of the pairs
	subset_pairs = random.choices(pairs, k = int(0.45 * len(pairs)))
	for pair in tqdm(subset_pairs):
		f1 = DIRS[0] + pair[0]
		f2 = DIRS[1] + pair[1]
		if not f1.lower().endswith('.zst'): continue
		for line, file_bytes_processed in read_lines_zst(f1):
			try:
				obj = json.loads(line)
				for subreddit in subreddit_names:
					if obj['subreddit'] == subreddit:
						if outs[subreddit+'_first']:
							outs[subreddit].write(json.dumps(obj))
							outs[subreddit+'_first'] = False
						else:
							outs[subreddit].write("," + json.dumps(obj))
			except (KeyError, json.JSONDecodeError) as err:
				print(f"Line decoding failed: {err}")
		if not f2.lower().endswith('.zst'): continue
		for line, file_bytes_processed in read_lines_zst(f2):
			try:
				obj = json.loads(line)
				for subreddit in subreddit_names:
					if obj['subreddit'] == subreddit:
						if outs[subreddit+'_first']:
							outs[subreddit].write(json.dumps(obj))
							outs[subreddit+'_first'] = False
						else:
							outs[subreddit].write("," + json.dumps(obj))	
			except (KeyError, json.JSONDecodeError) as err:
				print(f"Line decoding failed: {err}")
	for subreddit in subreddit_names:
		outs[subreddit].write("]")
		outs[subreddit].close()


