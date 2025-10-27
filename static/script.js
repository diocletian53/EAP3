// static/script.js

document.addEventListener('DOMContentLoaded', function () {
  const form = document.querySelector('form');
  const fileInput = document.querySelector('input[type=file]');
  const button = document.querySelector('button');

  form.addEventListener('submit', function (e) {
    if (!fileInput.value) {
      e.preventDefault();
      alert('Please upload an Excel file before submitting!');
    } else {
      button.disabled = true;
      button.textContent = 'Processing...';
      button.style.opacity = '0.7';
    }
  });
});
