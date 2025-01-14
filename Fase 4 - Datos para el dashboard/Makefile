dashboard:
	docker build -f Dockerfile -t streamlit .
	docker run -p 8501:8501 \
		-v ./scripts:/app/scripts \
		-v ./output:/app/output \
	 streamlit