from flask import Flask, request, jsonify
from transformers import pipeline 
app = Flask(__name__)
@app.route("/", methods=["GET","POST"])
def summarize():
    if request.method == 'Post':
        text = request.json.get('text')
        if text :
            summarizer = pipeline('sumarization')
            summary = summarizer(text)[0]['summary_text']
            response = {
                'original Text': text,
                'Summary': summary
            }
            return jsonify(response), 200
        else:
            return jsonify({'error': 'No text provided.'}), 400
    else:
        return jsonify({'message': 'Success'}), 200
if __name__ == '__main__':
    app.run()
        